package capture

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func testConfig() Config {
	return Config{Enabled: true, Instance: "test", Revision: "r1", Socket: "/tmp/private/capture.sock", Bindings: []Binding{{Key: "test-Dev-key-123456789", Destination: "dev", Deployment: "https://dev.convex.site", Environment: "dev"}}, QueueBytes: 2 << 20, MaxActive: 10}
}
func testHeaders() http.Header {
	return http.Header{"Authorization": {"Bearer test-Dev-key-123456789"}, "X-Meshix-Capture-Destination": {"dev"}, "X-Meshix-Capture-Revision": {"r1"}, "X-Meshix-Capture-Route": {"POST /v1/messages"}, "X-Meshix-Deployment": {"https://dev.convex.site"}}
}
func TestExactScope(t *testing.T) {
	for name, change := range map[string]func(http.Header){
		"personal":           func(h http.Header) { h.Set("Authorization", "Bearer personal-key-123456789") },
		"case":               func(h http.Header) { h.Set("Authorization", "Bearer test-dev-key-123456789") },
		"spoof":              func(h http.Header) { h.Set("X-Meshix-Capture-Destination", "prod") },
		"missing":            func(h http.Header) { h.Del("X-Meshix-Deployment") },
		"conflict":           func(h http.Header) { h.Set("X-Api-Key", "personal-key-123456789") },
		"duplicate":          func(h http.Header) { h["authorization"] = h["Authorization"] },
		"duplicateAuthority": func(h http.Header) { h["x-meshix-capture-destination"] = []string{"dev"} },
		"googleCarrier":      func(h http.Header) { h.Set("X-Goog-Api-Key", "personal-key-123456789") },
	} {
		t.Run(name, func(t *testing.T) {
			h := testHeaders()
			change(h)
			if _, why := Scope(testConfig(), h); why == "" {
				t.Fatal("enrolled invalid scope")
			}
		})
	}
	for _, carrier := range []string{"bearer", "api-key", "both"} {
		h := testHeaders()
		if carrier != "bearer" {
			h.Set("X-Api-Key", testConfig().Bindings[0].Key)
		}
		if carrier == "api-key" {
			h.Del("Authorization")
		}
		if b, reason := Scope(testConfig(), h); reason != "" || b.Destination != "dev" {
			t.Fatalf("%s rejected", carrier)
		}
	}
}
func TestOutboxRestartReplayConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "events.db")
	o, err := OpenOutbox(path, 4<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	e := Observation{SchemaVersion: 1, Destination: "dev", Instance: "instance", Boot: "boot", RequestID: "req", Sequence: 1, Kind: "request", Route: "POST /v1/messages", Body: []byte(`{"text":"hello"}`)}
	e.ContentBytes = len(e.Body)
	e.ContentSHA256 = Digest(e.Body)
	raw, _ := json.Marshal(e)
	send := func(b []byte) int {
		t.Helper()
		rec := httptest.NewRecorder()
		o.ServeHTTP(rec, httptest.NewRequest("POST", "/events", bytes.NewReader(b)))
		return rec.Code
	}
	if code := send(raw); code != 200 {
		t.Fatalf("first %d", code)
	}
	o.Close()
	o, err = OpenOutbox(path, 4<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if code := send(raw); code != 200 {
		t.Fatalf("replay %d", code)
	}
	e.Body = []byte(`{"text":"changed"}`)
	e.ContentBytes = len(e.Body)
	e.ContentSHA256 = Digest(e.Body)
	other, _ := json.Marshal(e)
	if code := send(other); code != 409 {
		t.Fatalf("conflict %d", code)
	}
	var n int
	o.db.QueryRow("select count(*) from events").Scan(&n)
	if n != 1 {
		t.Fatal("duplicate persisted")
	}
}
func TestPersonalAndUnownedNeverQueued(t *testing.T) {
	c := testConfig()
	e := NewEngine(c)
	defer e.Close()
	h := Hook{RequestID: "r", Headers: testHeaders(), Body: json.RawMessage(`"cGVyc29uYWw="`)}
	h.Headers.Set("Authorization", "Bearer personal-key-123456789")
	raw, _ := json.Marshal(h)
	e.Observe("request.intercept_before", raw)
	e.Observe("request.intercept_after", raw)
	e.Observe("request.complete", raw)
	e.Observe("response.intercept_stream_chunk", raw)
	if len(e.scopes) != 0 || e.bytes.Load() != 0 {
		t.Fatal("personal/unowned call retained")
	}
}
