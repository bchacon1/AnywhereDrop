package turn

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCredentialsMatchCoturnFormula(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	u, p := Credentials("s3cret", "anywheredrop", now, 10*time.Minute)
	if u != "1800000600:anywheredrop" {
		t.Fatalf("username = %q", u)
	}
	mac := hmac.New(sha1.New, []byte("s3cret"))
	mac.Write([]byte(u))
	if want := base64.StdEncoding.EncodeToString(mac.Sum(nil)); p != want {
		t.Fatalf("password = %q, want %q", p, want)
	}
	// A different secret yields a different password: coturn would reject it.
	if _, p2 := Credentials("other", "anywheredrop", now, 10*time.Minute); p2 == p {
		t.Fatal("password does not depend on secret")
	}
}

func TestServersWithoutTurn(t *testing.T) {
	s := Servers(Config{StunURLs: []string{"stun:a:3478"}}, time.Now())
	if len(s) != 1 || s[0].Username != "" {
		t.Fatalf("servers = %+v", s)
	}
}

func TestHandler(t *testing.T) {
	h := Handler(Config{StunURLs: []string{"stun:a:3478"}, TurnURLs: []string{"turn:b:3478?transport=udp"}, Secret: "x", TTL: 5 * time.Minute, Label: "ad"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/ice-config", nil))
	var body struct {
		IceServers []IceServer `json:"iceServers"`
		TTLSeconds int         `json:"ttlSeconds"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.IceServers) != 2 || body.TTLSeconds != 300 || !strings.HasSuffix(body.IceServers[1].Username, ":ad") || body.IceServers[1].Credential == "" {
		t.Fatalf("body = %+v", body)
	}
}

func TestParseURLs(t *testing.T) {
	if got := ParseURLs(" a , ,b"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
}
