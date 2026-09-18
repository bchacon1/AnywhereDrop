package signaling

import (
	"context"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"anywheredrop/backend/internal/bus"
	"anywheredrop/backend/internal/rooms"
)

// Config for the signaling handler. Timer values are the provisional
// defaults from architecture-decisions.md Part B.
type Config struct {
	Rooms              rooms.Config
	InstanceID         string
	CodeLength         int
	JoinLimitPerMinute int
	AttachTimeout      time.Duration // first message must arrive within this
	OriginPatterns     []string      // allowed Origin hosts for the upgrade; empty = same-origin only
}

// DefaultConfig returns the defaults.
func DefaultConfig() Config {
	return Config{
		Rooms:              rooms.DefaultConfig(),
		InstanceID:         "local",
		CodeLength:         6,
		JoinLimitPerMinute: 10,
		AttachTimeout:      10 * time.Second,
	}
}

// Handler upgrades /ws requests and runs one session per connection.
type Handler struct {
	rooms   rooms.Rooms
	bus     bus.Bus
	cfg     Config
	limiter *ipLimiter
	logf    func(format string, args ...any)
}

// New wires a handler to a store and a bus.
func New(r rooms.Rooms, b bus.Bus, cfg Config) *Handler {
	return &Handler{
		rooms:   r,
		bus:     b,
		cfg:     cfg,
		limiter: newIPLimiter(cfg.JoinLimitPerMinute, time.Minute),
		logf:    log.Printf,
	}
}

// ServeHTTP accepts the WebSocket and hands it to a session. It returns when
// the session ends; net/http gives each connection its own goroutine.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: h.cfg.OriginPatterns,
	})
	if err != nil {
		h.logf("ws accept: %v", err)
		return
	}
	s := newSession(h, conn, clientIP(r))
	s.run(r.Context())
}

// clientIP is the rate-limit key: the first X-Forwarded-For hop when behind
// a load balancer, else the remote address without the port.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// RunJanitors runs periodic cleanup for the limiter until ctx ends. The
// rooms store has its own janitor; main.go starts both.
func (h *Handler) RunJanitors(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.limiter.sweep()
		}
	}
}
