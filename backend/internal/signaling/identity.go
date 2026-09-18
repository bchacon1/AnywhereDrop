package signaling

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// codeAlphabet excludes look-alikes (0/O, 1/I/L) so codes can be read aloud
// and typed on a phone. 30 symbols; with codeLength 6 that is 30^6 = 729,000,000
// codes. Brute force: a waiting room lives WaitTTL (10 min) and joins are
// limited to JoinLimitPerMinute (10) per IP, so one IP gets ~100 guesses per
// room lifetime; the chance of hitting one of R waiting rooms is about
// 100*R / 7.29e8 (R=1000 -> ~1.4e-4). Revisit if the limits change.
const codeAlphabet = "ABCDEFGHJKMNPQRSTVWXYZ23456789"

// newRoomCode returns a random code of n symbols from codeAlphabet.
func newRoomCode(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	out := make([]byte, n)
	for i, b := range buf {
		// Slight modulo bias is irrelevant at this entropy; the code is a
		// pairing handle, not a key.
		out[i] = codeAlphabet[int(b)%len(codeAlphabet)]
	}
	return string(out), nil
}

// newPeerID returns 16 random bytes as hex: opaque, safe to log.
func newPeerID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// newPeerToken returns 32 random bytes, base64url without padding. The
// server stores only its hash (system-design.md §6.1).
func newPeerToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// hashToken is what the rooms store keeps.
func hashToken(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

// codeForLog returns a short hash of a room code so logs can correlate a
// session without exposing a joinable code (system-design.md §6.1).
func codeForLog(code string) string {
	h := sha256.Sum256([]byte(code))
	return fmt.Sprintf("room:%x", h[:4])
}
