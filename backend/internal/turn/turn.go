// Package turn mints short-lived TURN credentials for coturn's
// `use-auth-secret` mode (the "REST API" scheme): the username is
// "<expiry-unix>:<label>" and the password is base64(HMAC-SHA1(secret,
// username)). coturn recomputes the HMAC and rejects expired usernames, so
// the frontend never holds a long-lived secret (architecture-decisions.md A7).
package turn

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// IceServer is one entry of RTCConfiguration.iceServers.
type IceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

// Config for credential minting.
type Config struct {
	StunURLs []string      // e.g. stun:stun.l.google.com:19302
	TurnURLs []string      // e.g. turn:turn.example.com:3478?transport=udp; empty = no TURN
	Secret   string        // coturn static-auth-secret
	TTL      time.Duration // credential lifetime
	Label    string        // free text inside the username; not an identity
}

// Credentials returns the username/password pair valid until now+ttl.
func Credentials(secret, label string, now time.Time, ttl time.Duration) (username, password string) {
	expiry := now.Add(ttl).Unix()
	username = fmt.Sprintf("%d:%s", expiry, label)
	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(username))
	password = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return username, password
}

// Servers builds the iceServers list: STUN entries first, then one TURN
// entry with fresh credentials if TURN is configured.
func Servers(cfg Config, now time.Time) []IceServer {
	var out []IceServer
	if len(cfg.StunURLs) > 0 {
		out = append(out, IceServer{URLs: cfg.StunURLs})
	}
	if len(cfg.TurnURLs) > 0 && cfg.Secret != "" {
		u, p := Credentials(cfg.Secret, cfg.Label, now, cfg.TTL)
		out = append(out, IceServer{URLs: cfg.TurnURLs, Username: u, Credential: p})
	}
	return out
}

// Handler serves GET /api/ice-config as {"iceServers":[...], "ttlSeconds":N}.
func Handler(cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(struct {
			IceServers []IceServer `json:"iceServers"`
			TTLSeconds int         `json:"ttlSeconds"`
		}{Servers(cfg, time.Now()), int(cfg.TTL / time.Second)})
	})
}

// ParseURLs splits a comma-separated env value into URLs, dropping blanks.
func ParseURLs(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
