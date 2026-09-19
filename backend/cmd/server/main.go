// Command server is the single AnywhereDrop backend binary: static frontend,
// /health, and the /ws signaling endpoint.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	busmem "anywheredrop/backend/internal/bus/memory"
	roomsmem "anywheredrop/backend/internal/rooms/memory"
	"anywheredrop/backend/internal/signaling"
	"anywheredrop/backend/internal/turn"
	"anywheredrop/backend/internal/web"
	static "anywheredrop/backend/web"
)

func main() {
	addr := ":" + envOr("PORT", "8080")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Phase 2: in-memory rooms and bus. Phase 9 selects Redis here by env.
	sigCfg := signaling.DefaultConfig()
	sigCfg.InstanceID = envOr("INSTANCE_ID", hostnameOr("local"))
	// Browser Origin hosts allowed to open /ws. The Vite dev server on :5173
	// proxies with its own Origin, so localhost is allowed by default.
	sigCfg.OriginPatterns = strings.Split(envOr("WS_ORIGINS", "localhost:*,127.0.0.1:*"), ",")
	// Per-IP join attempts per minute (system-design.md §6.2). Test suites that
	// drive many rooms from one host raise it; production keeps the default.
	if n, err := strconv.Atoi(envOr("JOIN_LIMIT_PER_MINUTE", "")); err == nil && n > 0 {
		sigCfg.JoinLimitPerMinute = n
	}

	store := roomsmem.New(sigCfg.Rooms)
	relay := busmem.New()
	sig := signaling.New(store, relay, sigCfg)
	go store.Janitor(ctx, 30*time.Second)
	go sig.RunJanitors(ctx, time.Minute)

	// Phase 6: ICE server config. STUN always; TURN only when TURN_URLS and
	// TURN_SECRET are set (coturn use-auth-secret). Credentials live TURN_TTL.
	turnCfg := turn.Config{
		StunURLs: turn.ParseURLs(envOr("STUN_URLS", "stun:stun.l.google.com:19302")),
		TurnURLs: turn.ParseURLs(envOr("TURN_URLS", "")),
		Secret:   envOr("TURN_SECRET", ""),
		TTL:      10 * time.Minute,
		Label:    "anywheredrop",
	}
	if ttl, err := time.ParseDuration(envOr("TURN_TTL", "")); err == nil && ttl > 0 {
		turnCfg.TTL = ttl
	}

	mux := http.NewServeMux()
	mux.Handle("GET /health", web.HealthHandler(web.HealthInfo{Status: "ok", Rooms: "memory", Bus: "memory"}))
	mux.Handle("GET /api/ice-config", turn.Handler(turnCfg))
	mux.Handle("GET /ws", sig)
	mux.Handle("/", static.Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("listening on %s (instance %s)", addr, sigCfg.InstanceID)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func hostnameOr(fallback string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return fallback
}
