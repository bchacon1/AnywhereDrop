// Package memory is the single-process Bus.
package memory

import (
	"context"
	"log"
	"sync"

	"anywheredrop/backend/internal/bus"
)

// Bus implements bus.Bus with in-process channels.
type Bus struct {
	mu   sync.Mutex
	subs map[string]map[*subscription]struct{}
}

// New returns an empty bus.
func New() *Bus { return &Bus{subs: map[string]map[*subscription]struct{}{}} }

type subscription struct {
	b    *Bus
	code string
	ch   chan bus.Envelope
	once sync.Once
}

func (s *subscription) Messages() <-chan bus.Envelope { return s.ch }

func (s *subscription) Close() {
	s.once.Do(func() {
		s.b.mu.Lock()
		defer s.b.mu.Unlock()
		delete(s.b.subs[s.code], s)
		if len(s.b.subs[s.code]) == 0 {
			delete(s.b.subs, s.code)
		}
		// Closed under the lock: Publish also sends under the lock, so a send
		// on a closed channel (a panic in Go) cannot happen.
		close(s.ch)
	})
}

// Subscribe registers before returning, so anything published afterwards is
// delivered (system-design.md §6.3 ordering contract).
func (b *Bus) Subscribe(_ context.Context, code string) (bus.Subscription, error) {
	s := &subscription{b: b, code: code, ch: make(chan bus.Envelope, 256)}
	b.mu.Lock()
	if b.subs[code] == nil {
		b.subs[code] = map[*subscription]struct{}{}
	}
	b.subs[code][s] = struct{}{}
	b.mu.Unlock()
	return s, nil
}

// Publish delivers to every current subscriber. A subscriber whose buffer is
// full loses the message; this mirrors Pub/Sub's at-most-once semantics and
// is logged so it is visible in tests.
func (b *Bus) Publish(_ context.Context, code string, env bus.Envelope) error {
	env.Room = code
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs[code] {
		// Non-blocking send while holding the lock: cheap (no I/O), and it
		// serialises against Close so the channel is never closed mid-send.
		select {
		case s.ch <- env:
		default:
			log.Printf("bus: dropping %s for room %s: subscriber buffer full", env.Kind, code)
		}
	}
	return nil
}
