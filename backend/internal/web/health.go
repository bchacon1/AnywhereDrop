// Package web holds the small HTTP handlers that are not signaling.
package web

import (
	"encoding/json"
	"net/http"
)

// HealthInfo is what /health reports. The rooms and bus fields name the
// active implementations so a deployment can be checked from outside
// (Phase 9 switches them to "redis").
type HealthInfo struct {
	Status string `json:"status"`
	Rooms  string `json:"rooms"`
	Bus    string `json:"bus"`
}

// HealthHandler returns a handler that always reports the given info.
func HealthHandler(info HealthInfo) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(info)
	})
}
