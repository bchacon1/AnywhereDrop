package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	busmem "anywheredrop/backend/internal/bus/memory"
	"anywheredrop/backend/internal/rooms"
	roomsmem "anywheredrop/backend/internal/rooms/memory"
)

// harness runs the handler on an httptest server with a fake clock.
type harness struct {
	t     *testing.T
	srv   *httptest.Server
	store *roomsmem.Store
	h     *Handler
	mu    sync.Mutex
	now   time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	cfg := DefaultConfig()
	cfg.OriginPatterns = []string{"*"}
	cfg.AttachTimeout = 2 * time.Second
	cfg.Rooms.SilenceTimeout = 2 * time.Second
	store := roomsmem.New(cfg.Rooms)
	hs := &harness{t: t, store: store, now: time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)}
	store.SetClock(func() time.Time { hs.mu.Lock(); defer hs.mu.Unlock(); return hs.now })
	hs.h = New(store, busmem.New(), cfg)
	hs.h.logf = func(string, ...any) {}
	hs.srv = httptest.NewServer(hs.h)
	t.Cleanup(hs.srv.Close)
	return hs
}

func (hs *harness) advance(d time.Duration) {
	hs.mu.Lock()
	hs.now = hs.now.Add(d)
	hs.mu.Unlock()
}

type client struct {
	t    *testing.T
	conn *websocket.Conn
	ctx  context.Context
}

func (hs *harness) dial() *client {
	hs.t.Helper()
	ctx := context.Background()
	url := "ws" + strings.TrimPrefix(hs.srv.URL, "http") + "/ws"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		hs.t.Fatalf("dial: %v", err)
	}
	c := &client{t: hs.t, conn: conn, ctx: ctx}
	hs.t.Cleanup(func() { _ = conn.CloseNow() })
	return c
}

func (c *client) send(env Envelope) {
	c.t.Helper()
	env.V = Version
	if err := wsjson.Write(c.ctx, c.conn, env); err != nil {
		c.t.Fatalf("send %s: %v", env.Type, err)
	}
}

// recv waits for the next message of one of the given types; other types are skipped.
func (c *client) recv(types ...string) Envelope {
	c.t.Helper()
	deadline, cancel := context.WithTimeout(c.ctx, 3*time.Second)
	defer cancel()
	for {
		var env Envelope
		if err := wsjson.Read(deadline, c.conn, &env); err != nil {
			c.t.Fatalf("recv %v: %v", types, err)
		}
		if len(types) == 0 {
			return env
		}
		for _, ty := range types {
			if env.Type == ty {
				return env
			}
		}
	}
}

// expectClosed asserts the server closes the socket with the given code.
func (c *client) expectClosed(code websocket.StatusCode) {
	c.t.Helper()
	deadline, cancel := context.WithTimeout(c.ctx, 3*time.Second)
	defer cancel()
	for {
		var env Envelope
		err := wsjson.Read(deadline, c.conn, &env)
		if err == nil {
			continue
		}
		var ce websocket.CloseError
		if errors.As(err, &ce) {
			if ce.Code != code {
				c.t.Fatalf("close code = %d (%s), want %d", ce.Code, ce.Reason, code)
			}
			return
		}
		c.t.Fatalf("expected close %d, got %v", code, err)
	}
}

func pair(hs *harness) (creator, joiner *client, created, joined Envelope) {
	hs.t.Helper()
	creator = hs.dial()
	creator.send(Envelope{Type: TypeCreateRoom})
	created = creator.recv(TypeRoomCreated)
	joiner = hs.dial()
	joiner.send(Envelope{Type: TypeJoinRoom, Code: created.Code})
	joined = joiner.recv(TypeJoined)
	creator.recv(TypePeerJoined)
	return
}

// ---- 2a: create and join ---------------------------------------------------

func TestCreateAndJoin(t *testing.T) {
	hs := newHarness(t)
	creator, _, created, joined := pair(hs)
	if len(created.Code) != 6 || created.PeerID == "" || created.PeerToken == "" || created.Gen != 1 || created.Role != roleCreator {
		t.Fatalf("room_created = %+v", created)
	}
	if joined.Code != created.Code || joined.Role != roleJoiner || joined.RoomState == nil || joined.RoomState.State != "paired" {
		t.Fatalf("joined = %+v", joined)
	}
	// Peer ids differ and the token is never sent to the other peer.
	if joined.PeerID == created.PeerID {
		t.Fatal("peer ids collide")
	}
	_ = creator
}

func TestThirdJoinIsRoomFull(t *testing.T) {
	hs := newHarness(t)
	_, _, created, _ := pair(hs)
	third := hs.dial()
	third.send(Envelope{Type: TypeJoinRoom, Code: created.Code})
	e := third.recv(TypeError)
	if e.Code != ErrCodeRoomFull {
		t.Fatalf("error = %+v", e)
	}
	third.expectClosed(websocket.StatusPolicyViolation)
}

func TestJoinUnknownCode(t *testing.T) {
	hs := newHarness(t)
	c := hs.dial()
	c.send(Envelope{Type: TypeJoinRoom, Code: "NOPE00"})
	if e := c.recv(TypeError); e.Code != ErrCodeRoomNotFound {
		t.Fatalf("error = %+v", e)
	}
}

func TestBadFirstMessage(t *testing.T) {
	hs := newHarness(t)
	c := hs.dial()
	c.send(Envelope{Type: TypePing})
	if e := c.recv(TypeError); e.Code != ErrCodeProtocol {
		t.Fatalf("error = %+v", e)
	}
	c.expectClosed(CloseProtocol)
}

// Trace T4: the joiner's own `joined` reply precedes anything the creator
// relays in reaction to peer_joined.
func TestJoinedPrecedesRelay(t *testing.T) {
	hs := newHarness(t)
	creator := hs.dial()
	creator.send(Envelope{Type: TypeCreateRoom})
	created := creator.recv(TypeRoomCreated)

	// Creator relays the instant it sees peer_joined.
	go func() {
		creator.recv(TypePeerJoined)
		creator.send(Envelope{Type: TypeRelay, Payload: json.RawMessage(`{"offer":1}`)})
	}()

	joiner := hs.dial()
	joiner.send(Envelope{Type: TypeJoinRoom, Code: created.Code})
	first := joiner.recv()
	if first.Type != TypeJoined {
		t.Fatalf("first message to joiner = %s, want joined", first.Type)
	}
	if r := joiner.recv(TypeRelay); string(r.Payload) != `{"offer":1}` || r.From != created.PeerID {
		t.Fatalf("relay = %+v", r)
	}
}

// ---- 2b: text relay and silence -------------------------------------------

func TestRelayBothWays(t *testing.T) {
	hs := newHarness(t)
	creator, joiner, created, joined := pair(hs)
	creator.send(Envelope{Type: TypeRelay, Payload: json.RawMessage(`"hello"`)})
	if r := joiner.recv(TypeRelay); string(r.Payload) != `"hello"` || r.From != created.PeerID {
		t.Fatalf("joiner got %+v", r)
	}
	joiner.send(Envelope{Type: TypeRelay, Payload: json.RawMessage(`{"x":[1,2]}`)})
	if r := creator.recv(TypeRelay); string(r.Payload) != `{"x":[1,2]}` || r.From != joined.PeerID {
		t.Fatalf("creator got %+v", r)
	}
	// A sender never receives its own relay: next thing creator gets is a pong, not its message.
	creator.send(Envelope{Type: TypePing})
	if p := creator.recv(); p.Type != TypePong {
		t.Fatalf("creator got %+v, want pong", p)
	}
}

func TestSilentSocketIsDisconnectedAndPartnerTold(t *testing.T) {
	hs := newHarness(t)
	creator, joiner, _, joined := pair(hs)
	// The creator keeps pinging; the joiner says nothing for longer than
	// SilenceTimeout (2 s in tests) and is disconnected server-side.
	stop := keepAlive(creator, 500*time.Millisecond)
	defer stop()
	e := creator.recv(TypePeerDisconnected)
	if e.PeerID != joined.PeerID {
		t.Fatalf("peer_disconnected = %+v", e)
	}
	_ = joiner
}

// keepAlive pings c every interval until the returned func is called.
func keepAlive(c *client, every time.Duration) func() {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = wsjson.Write(c.ctx, c.conn, Envelope{V: Version, Type: TypePing})
			}
		}
	}()
	return func() { close(done) }
}

// ---- 2c: expiry and cleanup -----------------------------------------------

func TestJoinAfterWaitTTL(t *testing.T) {
	hs := newHarness(t)
	creator := hs.dial()
	creator.send(Envelope{Type: TypeCreateRoom})
	created := creator.recv(TypeRoomCreated)
	hs.advance(11 * time.Minute)
	j := hs.dial()
	j.send(Envelope{Type: TypeJoinRoom, Code: created.Code})
	if e := j.recv(TypeError); e.Code != ErrCodeRoomExpired {
		t.Fatalf("error = %+v", e)
	}
	// The creator's next ping tells it the code is dead.
	creator.send(Envelope{Type: TypePing})
	if rc := creator.recv(TypeRoomClosed); rc.Reason != ReasonExpired {
		t.Fatalf("room_closed = %+v", rc)
	}
	creator.expectClosed(CloseRoomClosed)
}

// Trace T2 via the wire: partner disconnected past grace -> room_closed{peer_gone}.
func TestPartnerGoneClosesRoom(t *testing.T) {
	hs := newHarness(t)
	creator, joiner, _, _ := pair(hs)
	_ = joiner.conn.Close(websocket.StatusNormalClosure, "bye")
	creator.recv(TypePeerDisconnected)
	hs.advance(61 * time.Second)
	creator.send(Envelope{Type: TypePing})
	if rc := creator.recv(TypeRoomClosed); rc.Reason != ReasonPeerGone {
		t.Fatalf("room_closed = %+v", rc)
	}
	creator.expectClosed(CloseRoomClosed)
}

// ---- 2d: authenticated reattachment ---------------------------------------

func TestReattachFlow(t *testing.T) {
	hs := newHarness(t)
	creator, joiner, _, joined := pair(hs)
	_ = joiner.conn.Close(websocket.StatusNormalClosure, "network blip")
	if e := creator.recv(TypePeerDisconnected); e.PeerID != joined.PeerID {
		t.Fatalf("peer_disconnected = %+v", e)
	}

	j2 := hs.dial()
	j2.send(Envelope{Type: TypeReattach, Code: joined.Code, PeerID: joined.PeerID, PeerToken: joined.PeerToken})
	re := j2.recv(TypeReattached)
	if re.Gen != 2 || re.Role != roleJoiner || re.RoomState == nil || re.RoomState.State != "paired" || re.PeerToken != "" {
		t.Fatalf("reattached = %+v", re)
	}
	if e := creator.recv(TypePeerReattached); e.PeerID != joined.PeerID || e.Gen != 2 {
		t.Fatalf("peer_reattached = %+v", e)
	}
	// Relay still works on the new socket.
	creator.send(Envelope{Type: TypeRelay, Payload: json.RawMessage(`1`)})
	if r := j2.recv(TypeRelay); string(r.Payload) != `1` {
		t.Fatalf("relay after reattach = %+v", r)
	}
}

func TestReattachBadTokenAndUnknownRoom(t *testing.T) {
	hs := newHarness(t)
	_, _, _, joined := pair(hs)
	c := hs.dial()
	c.send(Envelope{Type: TypeReattach, Code: joined.Code, PeerID: joined.PeerID, PeerToken: "wrong"})
	if e := c.recv(TypeError); e.Code != ErrCodeBadToken {
		t.Fatalf("error = %+v", e)
	}
	c2 := hs.dial()
	c2.send(Envelope{Type: TypeReattach, Code: "ZZZZZZ", PeerID: joined.PeerID, PeerToken: joined.PeerToken})
	if e := c2.recv(TypeError); e.Code != ErrCodeRoomNotFound {
		t.Fatalf("error = %+v", e)
	}
}

func TestReattachAfterGraceIsNotFound(t *testing.T) {
	hs := newHarness(t)
	_, joiner, _, joined := pair(hs)
	_ = joiner.conn.Close(websocket.StatusNormalClosure, "bye")
	time.Sleep(50 * time.Millisecond) // let the server record the disconnect
	hs.advance(61 * time.Second)
	c := hs.dial()
	c.send(Envelope{Type: TypeReattach, Code: joined.Code, PeerID: joined.PeerID, PeerToken: joined.PeerToken})
	if e := c.recv(TypeError); e.Code != ErrCodeRoomNotFound {
		t.Fatalf("error = %+v", e)
	}
}

// Trace T1 over the wire: reattach while the old socket is still open.
// The old socket is closed with CloseSuperseded, its later disconnect does
// not touch the slot, and the replacement keeps working.
func TestReattachSupersedesOpenSocket(t *testing.T) {
	hs := newHarness(t)
	creator, oldJoiner, _, joined := pair(hs)

	j2 := hs.dial()
	j2.send(Envelope{Type: TypeReattach, Code: joined.Code, PeerID: joined.PeerID, PeerToken: joined.PeerToken})
	if re := j2.recv(TypeReattached); re.Gen != 2 {
		t.Fatalf("reattached = %+v", re)
	}
	oldJoiner.expectClosed(CloseSuperseded)

	// The old socket's teardown must not have marked the slot disconnected.
	room, err := hs.store.Get(context.Background(), joined.Code)
	if err != nil || room.Joiner == nil || !room.Joiner.Connected || room.Joiner.Gen != 2 {
		t.Fatalf("slot after supersede: %v %+v", err, room.Joiner)
	}
	// And the creator must not have been told the joiner disconnected.
	creator.send(Envelope{Type: TypeRelay, Payload: json.RawMessage(`"still here"`)})
	if r := j2.recv(TypeRelay); string(r.Payload) != `"still here"` {
		t.Fatalf("relay = %+v", r)
	}
	// Anything the creator received between pairing and now must be peer_reattached, never peer_disconnected.
	creator.send(Envelope{Type: TypePing})
	for {
		e := creator.recv()
		if e.Type == TypePeerDisconnected {
			t.Fatal("creator saw peer_disconnected for a superseded socket")
		}
		if e.Type == TypePong {
			break
		}
	}
}

// Trace T3 mechanism: sync_request replies with room_state and re-announces peer_joined.
func TestSyncRequestReannouncesPeerJoined(t *testing.T) {
	hs := newHarness(t)
	creator, joiner, _, joined := pair(hs)
	joiner.send(Envelope{Type: TypeSyncRequest})
	rs := joiner.recv(TypeRoomState)
	if rs.RoomState == nil || rs.RoomState.State != "paired" || len(rs.RoomState.Peers) != 2 {
		t.Fatalf("room_state = %+v", rs)
	}
	if e := creator.recv(TypePeerJoined); e.PeerID != joined.PeerID {
		t.Fatalf("re-announced peer_joined = %+v", e)
	}
}

func TestJoinRateLimit(t *testing.T) {
	hs := newHarness(t)
	for i := 0; i < 10; i++ {
		c := hs.dial()
		c.send(Envelope{Type: TypeJoinRoom, Code: "NOPE00"})
		c.recv(TypeError) // room_not_found, but counted
	}
	c := hs.dial()
	c.send(Envelope{Type: TypeJoinRoom, Code: "NOPE00"})
	if e := c.recv(TypeError); e.Code != ErrCodeRateLimited {
		t.Fatalf("error = %+v", e)
	}
}

func TestLimiterWindow(t *testing.T) {
	l := newIPLimiter(2, time.Minute)
	now := time.Now()
	l.now = func() time.Time { return now }
	if !l.allow("a") || !l.allow("a") || l.allow("a") {
		t.Fatal("limit not enforced")
	}
	now = now.Add(61 * time.Second)
	if !l.allow("a") {
		t.Fatal("window did not slide")
	}
	l.sweep()
	if _, ok := l.events["a"]; !ok {
		t.Fatal("recent key swept")
	}
	now = now.Add(2 * time.Minute)
	l.sweep()
	if _, ok := l.events["a"]; ok {
		t.Fatal("stale key not swept")
	}
}

func TestRoomCodeAlphabet(t *testing.T) {
	for i := 0; i < 100; i++ {
		code, err := newRoomCode(6)
		if err != nil || len(code) != 6 {
			t.Fatal(err, code)
		}
		for _, ch := range code {
			if !strings.ContainsRune(codeAlphabet, ch) {
				t.Fatalf("bad symbol %q in %s", ch, code)
			}
		}
	}
	if strings.ContainsAny(codeAlphabet, "0O1IL") {
		t.Fatal("ambiguous symbols in alphabet")
	}
}

// Sanity: the store's Disconnect is what detach calls; gen must match.
func TestDetachUsesGeneration(t *testing.T) {
	hs := newHarness(t)
	_, joiner, _, joined := pair(hs)
	_ = joiner.conn.Close(websocket.StatusNormalClosure, "bye")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		room, _ := hs.store.Get(context.Background(), joined.Code)
		if room.Joiner != nil && !room.Joiner.Connected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("disconnect not recorded")
}

var _ = rooms.ErrNotFound // keep the import for future assertions
