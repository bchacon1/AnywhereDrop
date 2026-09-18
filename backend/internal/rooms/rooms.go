// Package rooms defines the room lifecycle store (system-design.md §6).
//
// A room pairs exactly two peers. Each peer occupies a Slot that is
// identified by a server-issued peerID and protected by a secret token whose
// hash is stored here. Every attachment of a socket to a slot has a
// generation number; heartbeat and disconnect are compare-and-set on it so a
// superseded socket can never modify the replacement attachment.
package rooms

import (
	"context"
	"errors"
	"time"
)

// State of a room.
type State string

const (
	Waiting State = "waiting" // created, joiner slot empty
	Paired  State = "paired"  // both slots filled
	Closed  State = "closed"  // terminal; kept briefly so late callers get ErrClosed
)

// Errors returned by Rooms implementations.
var (
	ErrNotFound        = errors.New("room not found")
	ErrExists          = errors.New("room code already exists")
	ErrFull            = errors.New("room already has two peers")
	ErrExpired         = errors.New("room expired before pairing")
	ErrClosed          = errors.New("room closed")
	ErrBadToken        = errors.New("bad peer token")
	ErrStaleAttachment = errors.New("stale attachment generation")
)

// Slot is one peer's place in a room.
type Slot struct {
	PeerID         string
	TokenHash      []byte // sha256(peerToken); the token itself is never stored
	Gen            int    // attachment generation: 1 at create/join, +1 per reattach
	Connected      bool
	InstanceID     string // backend instance holding the live socket
	LastSeen       time.Time
	DisconnectedAt time.Time // zero unless Connected == false
}

// Room is a snapshot of room state.
type Room struct {
	Code      string
	CreatedAt time.Time
	State     State
	Creator   Slot
	Joiner    *Slot // nil while Waiting
}

// Partner returns the other peer's slot, or nil.
func (r Room) Partner(peerID string) *Slot {
	if r.Creator.PeerID == peerID {
		return r.Joiner
	}
	if r.Joiner != nil && r.Joiner.PeerID == peerID {
		c := r.Creator
		return &c
	}
	return nil
}

// HeartbeatResult reports what the heartbeat found besides refreshing lastSeen.
type HeartbeatResult struct {
	PartnerGone bool // the room was closed by this heartbeat because the partner is gone
	Expired     bool // waiting room is past WaitTTL; the code can no longer be joined
}

// Config holds the lifecycle timers (architecture-decisions.md Part B, provisional).
type Config struct {
	WaitTTL        time.Duration // waiting room is unjoinable after this
	SilenceTimeout time.Duration // no message for this long => treated as disconnected
	ReattachGrace  time.Duration // disconnected slot may be reclaimed within this
}

// DefaultConfig returns the provisional defaults from the design docs.
func DefaultConfig() Config {
	return Config{
		WaitTTL:        10 * time.Minute,
		SilenceTimeout: 45 * time.Second,
		ReattachGrace:  60 * time.Second,
	}
}

// Gone reports whether a slot should be treated as permanently gone at now.
// Rule (system-design.md §6.2): disconnected longer than the grace, or still
// marked connected but silent longer than silence+grace (an instance died
// before recording the disconnect).
func (c Config) Gone(s Slot, now time.Time) bool {
	if !s.Connected {
		return now.Sub(s.DisconnectedAt) > c.ReattachGrace
	}
	return now.Sub(s.LastSeen) > c.SilenceTimeout+c.ReattachGrace
}

// Rooms is the store interface. Every implementation must make Join atomic
// (concurrent joins: exactly one succeeds) and must make Heartbeat and
// Disconnect no-ops that return ErrStaleAttachment when gen != slot.Gen.
type Rooms interface {
	// Create stores a new waiting room. creator.Gen is set to 1.
	Create(ctx context.Context, code string, creator Slot) error
	// Join fills the joiner slot if the room is waiting and not expired. joiner.Gen is set to 1.
	Join(ctx context.Context, code string, joiner Slot) (Room, error)
	// Reattach re-associates a peer with its slot when the token hash matches,
	// increments the generation, and marks the slot connected on instance.
	Reattach(ctx context.Context, code, peerID string, tokenHash []byte, instance string) (Room, int, error)
	// Heartbeat refreshes lastSeen and evaluates the partner's liveness.
	Heartbeat(ctx context.Context, code, peerID string, gen int) (HeartbeatResult, error)
	// Disconnect marks the slot disconnected at now.
	Disconnect(ctx context.Context, code, peerID string, gen int) error
	// Close marks the room closed.
	Close(ctx context.Context, code string) error
	// Get returns a snapshot.
	Get(ctx context.Context, code string) (Room, error)
}
