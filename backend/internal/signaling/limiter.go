package signaling

import (
	"sync"
	"time"
)

// ipLimiter allows at most limit events per window per key. It is a small
// sliding-window counter; there is no need for a library.
type ipLimiter struct {
	limit  int
	window time.Duration
	now    func() time.Time

	mu     sync.Mutex
	events map[string][]time.Time
}

func newIPLimiter(limit int, window time.Duration) *ipLimiter {
	return &ipLimiter{limit: limit, window: window, now: time.Now, events: map[string][]time.Time{}}
}

// allow records an event for key and reports whether it is within the limit.
func (l *ipLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cutoff := now.Add(-l.window)
	kept := l.events[key][:0]
	for _, t := range l.events[key] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= l.limit {
		l.events[key] = kept
		return false
	}
	l.events[key] = append(kept, now)
	return true
}

// sweep drops keys with no recent events so the map does not grow forever.
func (l *ipLimiter) sweep() {
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := l.now().Add(-l.window)
	for k, ts := range l.events {
		if len(ts) == 0 || !ts[len(ts)-1].After(cutoff) {
			delete(l.events, k)
		}
	}
}
