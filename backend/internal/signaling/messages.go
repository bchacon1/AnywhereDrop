// Package signaling implements the /ws endpoint: room lifecycle commands,
// heartbeat, and opaque relay between the two peers of a room. The wire
// format is the JSON envelope from system-design.md §2 / transfer-protocol.md §2.
package signaling

import "encoding/json"

// Version is the envelope version. Unknown versions are rejected.
const Version = 1

// Client -> server message types.
const (
	TypeCreateRoom  = "create_room"
	TypeJoinRoom    = "join_room"
	TypeReattach    = "reattach"
	TypeRelay       = "relay"
	TypePing        = "ping"
	TypeSyncRequest = "sync_request"
)

// Server -> client message types.
const (
	TypeRoomCreated      = "room_created"
	TypeJoined           = "joined"
	TypeReattached       = "reattached"
	TypePeerJoined       = "peer_joined"
	TypePeerDisconnected = "peer_disconnected"
	TypePeerReattached   = "peer_reattached"
	TypeRoomClosed       = "room_closed"
	TypePong             = "pong"
	TypeRoomState        = "room_state"
	TypeError            = "error"
)

// Error codes carried in Envelope.Code when Type == TypeError.
const (
	ErrCodeRoomNotFound       = "room_not_found"
	ErrCodeRoomFull           = "room_full"
	ErrCodeRoomExpired        = "room_expired"
	ErrCodeRoomClosed         = "room_closed"
	ErrCodeBadToken           = "bad_token"
	ErrCodeRateLimited        = "rate_limited"
	ErrCodeServiceUnavailable = "service_unavailable"
	ErrCodeProtocol           = "protocol"
)

// Reasons carried in room_closed.
const (
	ReasonPeerGone = "peer_gone"
	ReasonExpired  = "expired"
	ReasonClosed   = "closed"
)

// WebSocket close codes in the application range (4000-4999).
const (
	CloseSuperseded = 4001 // a newer attachment for this peer exists
	CloseRoomClosed = 4002 // the room ended
	CloseProtocol   = 4003 // client violated the protocol
)

// Envelope is every message in both directions. Which fields are set
// depends on Type; unused fields are omitted from the JSON.
type Envelope struct {
	V    int    `json:"v"`
	Type string `json:"type"`

	Code      string     `json:"code,omitempty"`      // room code, or error code when Type == error
	PeerID    string     `json:"peerId,omitempty"`    // own id in replies; subject in peer_* events
	PeerToken string     `json:"peerToken,omitempty"` // secret: only in room_created/joined (server->client) and reattach (client->server)
	Gen       int        `json:"gen,omitempty"`       // attachment generation
	Role      string     `json:"role,omitempty"`      // "creator" | "joiner"
	RoomState *RoomState `json:"roomState,omitempty"`

	From    string          `json:"from,omitempty"`    // relay: sending peer id
	Payload json.RawMessage `json:"payload,omitempty"` // relay: opaque client JSON

	Reason  string `json:"reason,omitempty"`  // room_closed
	Message string `json:"message,omitempty"` // error
}

// RoomState is the snapshot sent in room_created/joined/reattached and on
// sync_request, so a client can reconcile after reattach or resync.
type RoomState struct {
	State string      `json:"state"`
	Peers []PeerState `json:"peers"`
}

// PeerState is one slot as seen by clients. No token hash, no instance id.
type PeerState struct {
	PeerID    string `json:"peerId"`
	Role      string `json:"role"`
	Connected bool   `json:"connected"`
	Gen       int    `json:"gen"`
}

func errorEnvelope(code, message string) Envelope {
	return Envelope{V: Version, Type: TypeError, Code: code, Message: message}
}
