// Package memory is the single-process Rooms implementation: a map behind a
// mutex plus a janitor. It exists so Phase 9 can demonstrate what breaks with
// two instances before Redis is introduced.
package memory

import (
	"bytes"
	"context"
	"sync"
	"time"

	"anywheredrop/backend/internal/rooms"
)

// Store implements rooms.Rooms in memory.
type Store struct {
	cfg rooms.Config
	now func() time.Time

	mu    sync.Mutex
	rooms map[string]*rooms.Room
}

// New returns an empty store using the given config and wall clock.
func New(cfg rooms.Config) *Store {
	return &Store{cfg: cfg, now: time.Now, rooms: map[string]*rooms.Room{}}
}

// SetClock replaces the clock; tests use it to advance time deterministically.
func (s *Store) SetClock(now func() time.Time) { s.now = now }

func (s *Store) Create(_ context.Context, code string, creator rooms.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rooms[code]; ok {
		return rooms.ErrExists
	}
	now := s.now()
	creator.Gen = 1
	creator.Connected = true
	creator.LastSeen = now
	s.rooms[code] = &rooms.Room{Code: code, CreatedAt: now, State: rooms.Waiting, Creator: creator}
	return nil
}

func (s *Store) Join(_ context.Context, code string, joiner rooms.Slot) (rooms.Room, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[code]
	if !ok {
		return rooms.Room{}, rooms.ErrNotFound
	}
	now := s.now()
	switch {
	case r.State == rooms.Closed:
		return rooms.Room{}, rooms.ErrClosed
	case r.State == rooms.Paired:
		return rooms.Room{}, rooms.ErrFull
	case now.Sub(r.CreatedAt) > s.cfg.WaitTTL:
		return rooms.Room{}, rooms.ErrExpired
	}
	joiner.Gen = 1
	joiner.Connected = true
	joiner.LastSeen = now
	r.Joiner = &joiner
	r.State = rooms.Paired
	return snapshot(r), nil
}

func (s *Store) Reattach(_ context.Context, code, peerID string, tokenHash []byte, instance string) (rooms.Room, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[code]
	if !ok {
		return rooms.Room{}, 0, rooms.ErrNotFound
	}
	if r.State == rooms.Closed {
		return rooms.Room{}, 0, rooms.ErrClosed
	}
	slot := findSlot(r, peerID)
	if slot == nil || !bytes.Equal(slot.TokenHash, tokenHash) {
		// Same error for unknown peer and wrong token: no oracle for peer IDs.
		return rooms.Room{}, 0, rooms.ErrBadToken
	}
	now := s.now()
	if s.cfg.Gone(*slot, now) {
		// The grace has passed; the room is effectively dead for this peer.
		r.State = rooms.Closed
		return rooms.Room{}, 0, rooms.ErrNotFound
	}
	slot.Gen++
	slot.Connected = true
	slot.InstanceID = instance
	slot.LastSeen = now
	slot.DisconnectedAt = time.Time{}
	return snapshot(r), slot.Gen, nil
}

func (s *Store) Heartbeat(_ context.Context, code, peerID string, gen int) (rooms.HeartbeatResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[code]
	if !ok {
		return rooms.HeartbeatResult{}, rooms.ErrNotFound
	}
	if r.State == rooms.Closed {
		return rooms.HeartbeatResult{}, rooms.ErrClosed
	}
	slot := findSlot(r, peerID)
	if slot == nil {
		return rooms.HeartbeatResult{}, rooms.ErrNotFound
	}
	if slot.Gen != gen {
		return rooms.HeartbeatResult{}, rooms.ErrStaleAttachment
	}
	now := s.now()
	slot.LastSeen = now
	var res rooms.HeartbeatResult
	switch r.State {
	case rooms.Waiting:
		res.Expired = now.Sub(r.CreatedAt) > s.cfg.WaitTTL
	case rooms.Paired:
		if p := partnerOf(r, peerID); p != nil && s.cfg.Gone(*p, now) {
			r.State = rooms.Closed
			res.PartnerGone = true
		}
	}
	return res, nil
}

func (s *Store) Disconnect(_ context.Context, code, peerID string, gen int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[code]
	if !ok {
		return rooms.ErrNotFound
	}
	slot := findSlot(r, peerID)
	if slot == nil {
		return rooms.ErrNotFound
	}
	if slot.Gen != gen {
		return rooms.ErrStaleAttachment
	}
	if slot.Connected {
		slot.Connected = false
		slot.DisconnectedAt = s.now()
	}
	return nil
}

func (s *Store) Close(_ context.Context, code string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[code]
	if !ok {
		return rooms.ErrNotFound
	}
	r.State = rooms.Closed
	return nil
}

func (s *Store) Get(_ context.Context, code string) (rooms.Room, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[code]
	if !ok {
		return rooms.Room{}, rooms.ErrNotFound
	}
	return snapshot(r), nil
}

// Sweep applies the expiry rules once (system-design.md §6.2) and returns
// how many rooms were deleted. The janitor calls it periodically; tests call
// it directly.
func (s *Store) Sweep() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	deleted := 0
	for code, r := range s.rooms {
		switch r.State {
		case rooms.Closed:
			delete(s.rooms, code)
			deleted++
		case rooms.Waiting:
			if s.cfg.Gone(r.Creator, now) || now.Sub(r.CreatedAt) > s.cfg.WaitTTL+s.cfg.ReattachGrace {
				delete(s.rooms, code)
				deleted++
			}
		case rooms.Paired:
			cg, jg := s.cfg.Gone(r.Creator, now), s.cfg.Gone(*r.Joiner, now)
			if cg && jg {
				delete(s.rooms, code)
				deleted++
			} else if cg || jg {
				// One peer is gone; the other learns it from its next heartbeat (ErrClosed).
				r.State = rooms.Closed
			}
		}
	}
	return deleted
}

// Janitor runs Sweep every interval until ctx is done.
func (s *Store) Janitor(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Sweep()
		}
	}
}

// Len returns the number of rooms held (for tests and /metrics later).
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rooms)
}

func findSlot(r *rooms.Room, peerID string) *rooms.Slot {
	if r.Creator.PeerID == peerID {
		return &r.Creator
	}
	if r.Joiner != nil && r.Joiner.PeerID == peerID {
		return r.Joiner
	}
	return nil
}

func partnerOf(r *rooms.Room, peerID string) *rooms.Slot {
	if r.Creator.PeerID == peerID {
		return r.Joiner
	}
	return &r.Creator
}

func snapshot(r *rooms.Room) rooms.Room {
	out := *r
	if r.Joiner != nil {
		j := *r.Joiner
		out.Joiner = &j
	}
	return out
}
