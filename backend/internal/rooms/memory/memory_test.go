package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"anywheredrop/backend/internal/rooms"
)

func newStore() (*Store, *time.Time) {
	cfg := rooms.Config{WaitTTL: 10 * time.Minute, SilenceTimeout: 45 * time.Second, ReattachGrace: 60 * time.Second}
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	s := New(cfg)
	s.SetClock(func() time.Time { return now })
	return s, &now
}

func slot(id string) rooms.Slot { return rooms.Slot{PeerID: id, TokenHash: []byte("h-" + id)} }

func TestCreateJoinAndFull(t *testing.T) {
	s, _ := newStore()
	ctx := context.Background()
	if err := s.Create(ctx, "AAAAAA", slot("c")); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, "AAAAAA", slot("c2")); !errors.Is(err, rooms.ErrExists) {
		t.Fatalf("second create: %v", err)
	}
	r, err := s.Join(ctx, "AAAAAA", slot("j"))
	if err != nil || r.State != rooms.Paired || r.Joiner == nil || r.Joiner.Gen != 1 {
		t.Fatalf("join: %v %+v", err, r)
	}
	if _, err := s.Join(ctx, "AAAAAA", slot("j2")); !errors.Is(err, rooms.ErrFull) {
		t.Fatalf("third join: %v", err)
	}
	if _, err := s.Join(ctx, "NOPE", slot("x")); !errors.Is(err, rooms.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
}

func TestConcurrentJoinsExactlyOneWins(t *testing.T) {
	s, _ := newStore()
	ctx := context.Background()
	_ = s.Create(ctx, "RACE00", slot("c"))
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.Join(ctx, "RACE00", slot("j")); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("wins = %d, want 1", wins)
	}
}

func TestJoinAfterWaitTTLIsExpired(t *testing.T) {
	s, now := newStore()
	ctx := context.Background()
	_ = s.Create(ctx, "EXP000", slot("c"))
	*now = now.Add(11 * time.Minute)
	if _, err := s.Join(ctx, "EXP000", slot("j")); !errors.Is(err, rooms.ErrExpired) {
		t.Fatalf("join: %v", err)
	}
	res, err := s.Heartbeat(ctx, "EXP000", "c", 1)
	if err != nil || !res.Expired {
		t.Fatalf("heartbeat: %v %+v", err, res)
	}
}

// Trace T1 (transfer-protocol.md §7): a replacement attachment must not be
// modified by the old socket's heartbeat or disconnect.
func TestReattachSupersedesStaleAttachment(t *testing.T) {
	s, now := newStore()
	ctx := context.Background()
	_ = s.Create(ctx, "T1T1T1", slot("c"))
	_, _ = s.Join(ctx, "T1T1T1", slot("j"))

	// Old attachment (gen 1) drops; replacement reattaches on another instance.
	if err := s.Disconnect(ctx, "T1T1T1", "j", 1); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(5 * time.Second)
	r, gen, err := s.Reattach(ctx, "T1T1T1", "j", []byte("h-j"), "instance-Y")
	if err != nil || gen != 2 || !r.Joiner.Connected || r.Joiner.InstanceID != "instance-Y" {
		t.Fatalf("reattach: %v gen=%d %+v", err, gen, r.Joiner)
	}

	// Late heartbeat and disconnect from the old socket are rejected.
	before, _ := s.Get(ctx, "T1T1T1")
	*now = now.Add(5 * time.Second)
	if _, err := s.Heartbeat(ctx, "T1T1T1", "j", 1); !errors.Is(err, rooms.ErrStaleAttachment) {
		t.Fatalf("stale heartbeat: %v", err)
	}
	if err := s.Disconnect(ctx, "T1T1T1", "j", 1); !errors.Is(err, rooms.ErrStaleAttachment) {
		t.Fatalf("stale disconnect: %v", err)
	}
	after, _ := s.Get(ctx, "T1T1T1")
	if !after.Joiner.Connected || after.Joiner.Gen != 2 || !after.Joiner.LastSeen.Equal(before.Joiner.LastSeen) {
		t.Fatalf("replacement modified: before=%+v after=%+v", before.Joiner, after.Joiner)
	}
	// The live attachment still works.
	if _, err := s.Heartbeat(ctx, "T1T1T1", "j", 2); err != nil {
		t.Fatalf("live heartbeat: %v", err)
	}
}

func TestReattachBadToken(t *testing.T) {
	s, _ := newStore()
	ctx := context.Background()
	_ = s.Create(ctx, "TOKTOK", slot("c"))
	if _, _, err := s.Reattach(ctx, "TOKTOK", "c", []byte("wrong"), "i"); !errors.Is(err, rooms.ErrBadToken) {
		t.Fatalf("bad token: %v", err)
	}
	if _, _, err := s.Reattach(ctx, "TOKTOK", "nobody", []byte("h-c"), "i"); !errors.Is(err, rooms.ErrBadToken) {
		t.Fatalf("unknown peer: %v", err)
	}
}

func TestReattachAfterGraceIsNotFound(t *testing.T) {
	s, now := newStore()
	ctx := context.Background()
	_ = s.Create(ctx, "GRACE0", slot("c"))
	_, _ = s.Join(ctx, "GRACE0", slot("j"))
	_ = s.Disconnect(ctx, "GRACE0", "j", 1)
	*now = now.Add(61 * time.Second)
	if _, _, err := s.Reattach(ctx, "GRACE0", "j", []byte("h-j"), "i"); !errors.Is(err, rooms.ErrNotFound) {
		t.Fatalf("reattach after grace: %v", err)
	}
}

// Trace T2: the partner's heartbeat closes the room when the other slot is gone.
func TestHeartbeatDetectsPartnerGone(t *testing.T) {
	s, now := newStore()
	ctx := context.Background()
	_ = s.Create(ctx, "T2T2T2", slot("c"))
	_, _ = s.Join(ctx, "T2T2T2", slot("j"))

	// Case A: joiner disconnected, grace passes while creator heartbeats.
	_ = s.Disconnect(ctx, "T2T2T2", "j", 1)
	*now = now.Add(30 * time.Second)
	if res, err := s.Heartbeat(ctx, "T2T2T2", "c", 1); err != nil || res.PartnerGone {
		t.Fatalf("within grace: %v %+v", err, res)
	}
	*now = now.Add(31 * time.Second)
	res, err := s.Heartbeat(ctx, "T2T2T2", "c", 1)
	if err != nil || !res.PartnerGone {
		t.Fatalf("after grace: %v %+v", err, res)
	}
	if _, err := s.Heartbeat(ctx, "T2T2T2", "c", 1); !errors.Is(err, rooms.ErrClosed) {
		t.Fatalf("closed room heartbeat: %v", err)
	}

	// Case B: partner still marked connected but silent (its instance crashed).
	s2, now2 := newStore()
	_ = s2.Create(ctx, "CRASH0", slot("c"))
	_, _ = s2.Join(ctx, "CRASH0", slot("j"))
	*now2 = now2.Add(100 * time.Second) // < 45s + 60s
	if res, err := s2.Heartbeat(ctx, "CRASH0", "c", 1); err != nil || res.PartnerGone {
		t.Fatalf("silent but within limit: %v %+v", err, res)
	}
	*now2 = now2.Add(6 * time.Second) // now > 105s since joiner's lastSeen
	if res, err := s2.Heartbeat(ctx, "CRASH0", "c", 1); err != nil || !res.PartnerGone {
		t.Fatalf("silent past limit: %v %+v", err, res)
	}
}

func TestSweep(t *testing.T) {
	s, now := newStore()
	ctx := context.Background()
	_ = s.Create(ctx, "SWEEP1", slot("c")) // waiting, creator connected: stays until waitTTL+grace
	_ = s.Create(ctx, "SWEEP2", slot("c2"))
	_ = s.Disconnect(ctx, "SWEEP2", "c2", 1) // waiting, creator gone after grace
	_ = s.Create(ctx, "SWEEP3", slot("c3"))
	_, _ = s.Join(ctx, "SWEEP3", slot("j3"))
	_ = s.Close(ctx, "SWEEP3") // closed: deleted on next sweep

	if n := s.Sweep(); n != 1 || s.Len() != 2 {
		t.Fatalf("sweep 1: deleted=%d len=%d", n, s.Len())
	}
	*now = now.Add(61 * time.Second)
	if n := s.Sweep(); n != 1 || s.Len() != 1 {
		t.Fatalf("sweep 2: deleted=%d len=%d", n, s.Len())
	}
	*now = now.Add(11 * time.Minute)
	if n := s.Sweep(); n != 1 || s.Len() != 0 {
		t.Fatalf("sweep 3: deleted=%d len=%d", n, s.Len())
	}
}
