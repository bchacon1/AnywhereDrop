// Package bus is the per-room message relay between sessions. With one
// process it is a set of Go channels; in Phase 9 it becomes Redis Pub/Sub
// with the same contract: Subscribe returns only once the subscription is
// live, delivery is at-most-once, and order is preserved per publisher.
package bus

import (
	"context"
	"encoding/json"
)

// Kinds of envelope.
const (
	KindRelay            = "relay"             // opaque client payload (offer/answer/ice/text)
	KindPeerJoined       = "peer_joined"       // joiner arrived
	KindPeerDisconnected = "peer_disconnected" // a slot lost its socket
	KindPeerReattached   = "peer_reattached"   // a slot regained a socket
	KindSuperseded       = "superseded"        // a newer attachment for FromPeer exists; older sockets must close
	KindRoomClosed       = "room_closed"       // room is over
)

// Envelope is one message on a room's channel.
type Envelope struct {
	Room     string          `json:"room"`
	FromPeer string          `json:"from"`
	FromGen  int             `json:"gen"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

// Subscription delivers envelopes for one room.
type Subscription interface {
	Messages() <-chan Envelope
	Close()
}

// Bus is the relay interface.
type Bus interface {
	Subscribe(ctx context.Context, code string) (Subscription, error)
	Publish(ctx context.Context, code string, env Envelope) error
}
