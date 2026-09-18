package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"anywheredrop/backend/internal/bus"
	"anywheredrop/backend/internal/rooms"
)

const (
	roleCreator = "creator"
	roleJoiner  = "joiner"
)

// session is one WebSocket connection attached to one slot of one room.
// Two goroutines besides the caller: writeLoop (FIFO writer, so replies
// always precede announcements on the wire) and busLoop (relay in).
type session struct {
	h    *Handler
	conn *websocket.Conn
	ip   string

	ctx    context.Context
	cancel context.CancelFunc

	code   string
	peerID string
	gen    int
	role   string
	sub    bus.Subscription

	out chan Envelope

	// Close state. requestClose sets code/reason once and closes `closed`;
	// the writer goroutine then drains `out`, closes the socket, and cancels
	// the context. Owning the close in the writer guarantees every queued
	// message (an error, a room_closed) reaches the wire before the close frame.
	mu          sync.Mutex
	closeCode   websocket.StatusCode
	closeReason string
	closed      chan struct{}
	writerDone  chan struct{}
}

func newSession(h *Handler, conn *websocket.Conn, ip string) *session {
	return &session{
		h: h, conn: conn, ip: ip,
		out:        make(chan Envelope, 64),
		closed:     make(chan struct{}),
		writerDone: make(chan struct{}),
	}
}

// run is the whole life of the connection: attach, serve, clean up.
func (s *session) run(parent context.Context) {
	s.ctx, s.cancel = context.WithCancel(parent)
	defer s.cancel()
	go s.writeLoop()

	if !s.attach() {
		s.requestClose(CloseProtocol, "attach failed")
		s.awaitWriter()
		return
	}
	s.h.logf("%s attached peer=%s role=%s gen=%d instance=%s", codeForLog(s.code), s.peerID, s.role, s.gen, s.h.cfg.InstanceID)

	go s.busLoop()
	s.readLoop()
	s.detach()
	s.requestClose(websocket.StatusNormalClosure, "bye")
	s.awaitWriter()
	s.sub.Close()
}

// ---- attach phase --------------------------------------------------------

// attach reads the first message and performs create/join/reattach.
// Order, fixed by system-design.md §6.3: Subscribe -> state change -> reply
// on own socket -> announce on the bus.
func (s *session) attach() bool {
	first, err := s.readOne(s.h.cfg.AttachTimeout)
	if err != nil {
		s.requestClose(CloseProtocol, "expected create_room, join_room or reattach")
		return false
	}
	switch first.Type {
	case TypeCreateRoom:
		return s.attachCreate()
	case TypeJoinRoom:
		return s.attachJoin(first)
	case TypeReattach:
		return s.attachReattach(first)
	default:
		s.send(errorEnvelope(ErrCodeProtocol, "first message must be create_room, join_room or reattach"))
		s.requestClose(CloseProtocol, "bad first message")
		return false
	}
}

func (s *session) newIdentity() (peerID, token string, ok bool) {
	peerID, err := newPeerID()
	if err == nil {
		token, err = newPeerToken()
	}
	if err != nil {
		s.send(errorEnvelope(ErrCodeServiceUnavailable, "identity generation failed"))
		s.requestClose(websocket.StatusInternalError, "identity")
		return "", "", false
	}
	return peerID, token, true
}

func (s *session) attachCreate() bool {
	peerID, token, ok := s.newIdentity()
	if !ok {
		return false
	}
	slot := rooms.Slot{PeerID: peerID, TokenHash: hashToken(token), InstanceID: s.h.cfg.InstanceID}
	for attempt := 0; attempt < 5; attempt++ {
		code, err := newRoomCode(s.h.cfg.CodeLength)
		if err != nil {
			break
		}
		sub, err := s.h.bus.Subscribe(s.ctx, code)
		if err != nil {
			break
		}
		err = s.h.rooms.Create(s.ctx, code, slot)
		if errors.Is(err, rooms.ErrExists) {
			sub.Close()
			continue
		}
		if err != nil {
			sub.Close()
			break
		}
		s.code, s.peerID, s.gen, s.role, s.sub = code, peerID, 1, roleCreator, sub
		room, _ := s.h.rooms.Get(s.ctx, code)
		s.send(Envelope{V: Version, Type: TypeRoomCreated, Code: code, PeerID: peerID, PeerToken: token, Gen: 1, Role: roleCreator, RoomState: toRoomState(room)})
		return true
	}
	s.send(errorEnvelope(ErrCodeServiceUnavailable, "could not create room"))
	s.requestClose(websocket.StatusInternalError, "create")
	return false
}

func (s *session) attachJoin(msg Envelope) bool {
	if !s.h.limiter.allow(s.ip) {
		s.send(errorEnvelope(ErrCodeRateLimited, "too many join attempts"))
		s.requestClose(websocket.StatusPolicyViolation, "rate limited")
		return false
	}
	peerID, token, ok := s.newIdentity()
	if !ok {
		return false
	}
	code := msg.Code
	sub, err := s.h.bus.Subscribe(s.ctx, code)
	if err != nil {
		s.send(errorEnvelope(ErrCodeServiceUnavailable, "bus unavailable"))
		s.requestClose(websocket.StatusInternalError, "bus")
		return false
	}
	slot := rooms.Slot{PeerID: peerID, TokenHash: hashToken(token), InstanceID: s.h.cfg.InstanceID}
	room, err := s.h.rooms.Join(s.ctx, code, slot)
	if err != nil {
		sub.Close()
		s.send(errorEnvelope(joinErrorCode(err), err.Error()))
		s.requestClose(websocket.StatusPolicyViolation, "join failed")
		return false
	}
	s.code, s.peerID, s.gen, s.role, s.sub = code, peerID, 1, roleJoiner, sub
	// Reply first, then announce: the joiner must see `joined` before any
	// relayed offer that the announcement triggers (trace T4).
	s.send(Envelope{V: Version, Type: TypeJoined, Code: code, PeerID: peerID, PeerToken: token, Gen: 1, Role: roleJoiner, RoomState: toRoomState(room)})
	s.publish(bus.KindPeerJoined, nil)
	return true
}

func (s *session) attachReattach(msg Envelope) bool {
	if msg.Code == "" || msg.PeerID == "" || msg.PeerToken == "" {
		s.send(errorEnvelope(ErrCodeProtocol, "reattach requires code, peerId, peerToken"))
		s.requestClose(CloseProtocol, "bad reattach")
		return false
	}
	sub, err := s.h.bus.Subscribe(s.ctx, msg.Code)
	if err != nil {
		s.send(errorEnvelope(ErrCodeServiceUnavailable, "bus unavailable"))
		s.requestClose(websocket.StatusInternalError, "bus")
		return false
	}
	room, gen, err := s.h.rooms.Reattach(s.ctx, msg.Code, msg.PeerID, hashToken(msg.PeerToken), s.h.cfg.InstanceID)
	if err != nil {
		sub.Close()
		s.send(errorEnvelope(reattachErrorCode(err), err.Error()))
		s.requestClose(websocket.StatusPolicyViolation, "reattach failed")
		return false
	}
	s.code, s.peerID, s.gen, s.sub = msg.Code, msg.PeerID, gen, sub
	s.role = roleJoiner
	if room.Creator.PeerID == s.peerID {
		s.role = roleCreator
	}
	s.send(Envelope{V: Version, Type: TypeReattached, Code: s.code, PeerID: s.peerID, Gen: gen, Role: s.role, RoomState: toRoomState(room)})
	// Any older socket for this peer, on any instance, must close (trace T1).
	s.publish(bus.KindSuperseded, nil)
	s.publish(bus.KindPeerReattached, nil)
	return true
}

// ---- serve phase ---------------------------------------------------------

// readLoop handles client messages until the socket closes or the session
// is asked to close. Each read has the silence timeout as its deadline.
func (s *session) readLoop() {
	for {
		msg, err := s.readOne(s.h.cfg.Rooms.SilenceTimeout)
		if err != nil {
			return
		}
		switch msg.Type {
		case TypePing:
			s.handlePing()
		case TypeRelay:
			if len(msg.Payload) == 0 {
				s.send(errorEnvelope(ErrCodeProtocol, "relay requires payload"))
				continue
			}
			s.publish(bus.KindRelay, msg.Payload)
		case TypeSyncRequest:
			s.handleSyncRequest()
		default:
			s.send(errorEnvelope(ErrCodeProtocol, "unknown type "+msg.Type))
		}
		select {
		case <-s.closed:
			return
		default:
		}
	}
}

func (s *session) handlePing() {
	res, err := s.h.rooms.Heartbeat(s.ctx, s.code, s.peerID, s.gen)
	switch {
	case errors.Is(err, rooms.ErrStaleAttachment):
		s.requestClose(CloseSuperseded, "superseded")
	case errors.Is(err, rooms.ErrClosed), errors.Is(err, rooms.ErrNotFound):
		s.send(Envelope{V: Version, Type: TypeRoomClosed, Reason: ReasonClosed})
		s.requestClose(CloseRoomClosed, "room closed")
	case err != nil:
		s.send(errorEnvelope(ErrCodeServiceUnavailable, err.Error()))
	case res.PartnerGone:
		s.publish(bus.KindRoomClosed, nil)
		s.send(Envelope{V: Version, Type: TypeRoomClosed, Reason: ReasonPeerGone})
		s.requestClose(CloseRoomClosed, "peer gone")
	case res.Expired:
		_ = s.h.rooms.Close(s.ctx, s.code)
		s.send(Envelope{V: Version, Type: TypeRoomClosed, Reason: ReasonExpired})
		s.requestClose(CloseRoomClosed, "expired")
	default:
		s.send(Envelope{V: Version, Type: TypePong})
	}
}

// handleSyncRequest answers with the room snapshot and, if paired,
// re-announces peer_joined so a creator that missed it can start
// negotiation (transfer-protocol.md §6.4, trace T3).
func (s *session) handleSyncRequest() {
	room, err := s.h.rooms.Get(s.ctx, s.code)
	if err != nil {
		s.send(Envelope{V: Version, Type: TypeRoomClosed, Reason: ReasonClosed})
		s.requestClose(CloseRoomClosed, "room closed")
		return
	}
	s.send(Envelope{V: Version, Type: TypeRoomState, RoomState: toRoomState(room)})
	if room.State == rooms.Paired && s.role == roleJoiner {
		s.publish(bus.KindPeerJoined, nil)
	}
}

// busLoop forwards envelopes from the partner to this socket and handles
// the one envelope about *this* peer that matters: superseded.
func (s *session) busLoop() {
	for env := range s.sub.Messages() {
		if env.FromPeer == s.peerID {
			if env.Kind == bus.KindSuperseded && env.FromGen > s.gen {
				s.requestClose(CloseSuperseded, "superseded")
				return
			}
			continue
		}
		switch env.Kind {
		case bus.KindRelay:
			s.send(Envelope{V: Version, Type: TypeRelay, From: env.FromPeer, Payload: env.Payload})
		case bus.KindPeerJoined:
			s.send(Envelope{V: Version, Type: TypePeerJoined, PeerID: env.FromPeer})
		case bus.KindPeerDisconnected:
			s.send(Envelope{V: Version, Type: TypePeerDisconnected, PeerID: env.FromPeer})
		case bus.KindPeerReattached:
			s.send(Envelope{V: Version, Type: TypePeerReattached, PeerID: env.FromPeer, Gen: env.FromGen})
		case bus.KindRoomClosed:
			s.send(Envelope{V: Version, Type: TypeRoomClosed, Reason: ReasonPeerGone})
			s.requestClose(CloseRoomClosed, "room closed")
			return
		}
	}
}

// ---- detach --------------------------------------------------------------

// detach records the disconnect (compare-and-set on gen) and tells the
// partner, unless this socket was superseded, in which case the slot is
// still live under a newer generation and nothing must change.
func (s *session) detach() {
	if code, _ := s.closeState(); code == CloseSuperseded {
		return
	}
	err := s.h.rooms.Disconnect(context.Background(), s.code, s.peerID, s.gen)
	if err == nil {
		s.publish(bus.KindPeerDisconnected, nil)
	}
}

// ---- plumbing ------------------------------------------------------------

func (s *session) readOne(timeout time.Duration) (Envelope, error) {
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	defer cancel()
	var msg Envelope
	if err := wsjson.Read(ctx, s.conn, &msg); err != nil {
		return Envelope{}, err
	}
	if msg.V != Version {
		s.send(errorEnvelope(ErrCodeProtocol, "unsupported envelope version"))
		return Envelope{}, errors.New("bad version")
	}
	return msg, nil
}

// send enqueues for the writer goroutine. Non-blocking: a client that does
// not drain 64 messages is broken and is closed rather than allowed to
// stall the session.
func (s *session) send(env Envelope) {
	select {
	case s.out <- env:
	default:
		s.requestClose(websocket.StatusPolicyViolation, "client not reading")
	}
}

func (s *session) publish(kind string, payload json.RawMessage) {
	env := bus.Envelope{FromPeer: s.peerID, FromGen: s.gen, Kind: kind, Payload: payload}
	if err := s.h.bus.Publish(s.ctx, s.code, env); err != nil {
		s.send(errorEnvelope(ErrCodeServiceUnavailable, "signaling_degraded"))
	}
}

func (s *session) writeLoop() {
	defer close(s.writerDone)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.closed:
			s.drainAndClose()
			return
		case env := <-s.out:
			if !s.write(env) {
				s.requestClose(websocket.StatusAbnormalClosure, "write failed")
				s.drainAndClose()
				return
			}
		}
	}
}

func (s *session) write(env Envelope) bool {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	return wsjson.Write(ctx, s.conn, env) == nil
}

// drainAndClose writes whatever is still queued, then sends the close frame
// and cancels the session context (which unblocks the read loop).
func (s *session) drainAndClose() {
	for i := 0; i < cap(s.out); i++ {
		select {
		case env := <-s.out:
			if !s.write(env) {
				i = cap(s.out) // stop draining; socket is dead
			}
		default:
			i = cap(s.out)
		}
	}
	code, reason := s.closeState()
	_ = s.conn.Close(code, reason)
	s.cancel()
}

// requestClose records the close reason once and signals the writer.
func (s *session) requestClose(code websocket.StatusCode, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
		return
	default:
	}
	s.closeCode, s.closeReason = code, reason
	close(s.closed)
}

func (s *session) closeState() (websocket.StatusCode, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCode, s.closeReason
}

// awaitWriter waits for the writer to finish the close, with a bound so a
// wedged socket cannot leak the session.
func (s *session) awaitWriter() {
	select {
	case <-s.writerDone:
	case <-time.After(3 * time.Second):
		s.cancel()
		_ = s.conn.CloseNow()
	}
}

func toRoomState(r rooms.Room) *RoomState {
	st := &RoomState{State: string(r.State)}
	st.Peers = append(st.Peers, PeerState{PeerID: r.Creator.PeerID, Role: roleCreator, Connected: r.Creator.Connected, Gen: r.Creator.Gen})
	if r.Joiner != nil {
		st.Peers = append(st.Peers, PeerState{PeerID: r.Joiner.PeerID, Role: roleJoiner, Connected: r.Joiner.Connected, Gen: r.Joiner.Gen})
	}
	return st
}

func joinErrorCode(err error) string {
	switch {
	case errors.Is(err, rooms.ErrNotFound):
		return ErrCodeRoomNotFound
	case errors.Is(err, rooms.ErrFull):
		return ErrCodeRoomFull
	case errors.Is(err, rooms.ErrExpired):
		return ErrCodeRoomExpired
	case errors.Is(err, rooms.ErrClosed):
		return ErrCodeRoomClosed
	}
	return ErrCodeServiceUnavailable
}

func reattachErrorCode(err error) string {
	switch {
	case errors.Is(err, rooms.ErrBadToken):
		return ErrCodeBadToken
	case errors.Is(err, rooms.ErrNotFound):
		return ErrCodeRoomNotFound
	case errors.Is(err, rooms.ErrClosed):
		return ErrCodeRoomClosed
	}
	return ErrCodeServiceUnavailable
}
