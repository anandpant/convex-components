package capture

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
func TestFramedRedactionEverySplit(t *testing.T) {
	raw := []byte("event: message_delta\r\ndata: {\"text\":\"secret-value \\u00e9\",\"access_token\":\"not-in-known-set\",\"arguments\":\"{\\\"api_key\\\":\\\"nested\\\"}\"}\r\n\r\n")
	for split := 0; split <= len(raw); split++ {
		r := FrameRedactor{}
		a, _, _ := r.Feed(raw[:split], 1, true, false, []string{"secret-value"})
		b, _, gap := r.Feed(raw[split:], 2, true, true, []string{"secret-value"})
		out := string(append(a, b...))
		if gap != "" || strings.Contains(out, "secret-value") || strings.Contains(out, "not-in-known-set") || strings.Contains(out, "nested") || !strings.Contains(out, "[REDACTED]") {
			t.Fatalf("split %d unsafe or missing output: %q", split, gap)
		}
	}
	r := FrameRedactor{}
	out, _, gap := r.Feed([]byte("data: {\"text\":\"partial secret"), 1, true, true, nil)
	if len(out) != 0 || gap != "truncated_frame_withheld" {
		t.Fatal("truncated sensitive frame persisted")
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
	e.Observe("request.complete", raw)
	e.Observe("response.intercept_stream_chunk", raw)
	if len(e.scopes) != 0 || e.bytes.Load() != 0 {
		t.Fatal("personal/unowned call retained")
	}
}

func TestStockResponsesUndelimitedCandidate(t *testing.T) {
	r := FrameRedactor{allowUndelimited: true}
	frames := []string{"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}", "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}", "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}"}
	for i, frame := range frames {
		out, _, gap := r.Feed([]byte(frame), uint64(i+1), true, false, nil)
		if len(out) == 0 || gap != "" {
			t.Fatal("stock Responses candidate withheld")
		}
	}
	out, _, gap := r.Feed(nil, 4, true, true, nil)
	if len(out) != 0 || gap != "" {
		t.Fatal("false truncation")
	}
}

func TestRedactionDepthFailureWithholdsOriginal(t *testing.T) {
	nested := []byte(`{"arguments":"{\"api_key\":\"unknown-secret\"}"}`)
	if out, ok := scrubJSONDepth(nested, nil, 32); ok || len(out) != 0 {
		t.Fatal("failed nested redaction retained original credential")
	}
	deep := []byte(strings.Repeat(`{"value":`, 34) + `{"api_key":"unknown-secret"}` + strings.Repeat(`}`, 34))
	r := FrameRedactor{}
	out, _, gap := r.Feed(deep, 1, false, true, nil)
	if len(out) != 0 || gap != "invalid_or_unredactable_json_withheld" {
		t.Fatal("depth limit did not withhold the frame")
	}
	if out, ok := scrubJSON([]byte(`{"known-secret":"a"}`), []string{"known-secret"}); ok || len(out) != 0 {
		t.Fatal("secret object key persisted")
	}
}
func TestSemanticCredentialFragmentsNeverPersist(t *testing.T) {
	for split := 1; split < len("secret-value"); split++ {
		r := FrameRedactor{}
		var all []byte
		for index, part := range []string{"secret-value"[:split], "secret-value"[split:]} {
			payload, _ := json.Marshal(map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]string{"type": "text_delta", "text": part}})
			body := append([]byte("data: "), payload...)
			body = append(body, '\n', '\n')
			out, _, gap := r.Feed(body, uint64(index+1), true, index == 1, []string{"secret-value"})
			if gap != "" {
				t.Fatal(gap)
			}
			if index == 0 && len(out) != 0 {
				t.Fatal("ambiguous prefix released")
			}
			all = append(all, out...)
		}
		if strings.Contains(string(all), "secret-value") || !strings.Contains(string(all), "[REDACTED]") {
			t.Fatal("fragment redaction failed")
		}
	}
	r := FrameRedactor{}
	var all []byte
	for index, part := range []string{`{"api_`, `key":"not-in-known-set"}`} {
		payload, _ := json.Marshal(map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]string{"type": "input_json_delta", "partial_json": part}})
		body := append([]byte("data: "), payload...)
		body = append(body, '\n', '\n')
		out, _, gap := r.Feed(body, uint64(index+1), true, index == 1, nil)
		if gap != "" {
			t.Fatal(gap)
		}
		all = append(all, out...)
	}
	if strings.Contains(string(all), "not-in-known-set") || !strings.Contains(string(all), "[REDACTED]") {
		t.Fatal("serialized JSON credential fragments leaked")
	}
}
func TestStockOpenAIJSONChunks(t *testing.T) {
	r := FrameRedactor{allowJSON: true}
	out, from, gap := r.Feed([]byte(`{"id":"stock","choices":[{"index":0,"delta":{"content":"CAPTURE_OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`), 3, true, false, nil)
	if gap != "" || from == nil || *from != 3 || !bytes.HasPrefix(out, []byte("data: {")) || !bytes.HasSuffix(out, []byte("\n\n")) {
		t.Fatal("stock pre-framer JSON chunk lost")
	}
	tail, _, gap := r.Feed(nil, 4, true, true, nil)
	if len(tail) != 0 || gap != "" {
		t.Fatal("complete raw JSON chunk reported truncated")
	}
}
func TestRedactedCorrelationsCannotCreateFalseJoins(t *testing.T) {
	o := Observation{Model: "model-secret", TraceID: "trace-secret", Correlation: map[string]string{"runId": "run-secret", "requestId": "exact-request"}}
	redactMetadata(&o, []string{"secret"})
	if o.TraceID != "" || o.Correlation["runId"] != "" || o.Correlation["requestId"] != "exact-request" || len(o.CorrelationConflicts) != 2 {
		t.Fatal("redaction fabricated an indexed correlation")
	}
}
