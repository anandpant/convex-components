package capture

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercise the public hook adapter and its worker, including route selection,
// callback sequencing, framing metadata and the serialized Unix-socket output.
func captureResponsesCallbacks(t *testing.T, callbacks []string) []Observation {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "capture-sse-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	c := testConfig()
	c.Socket = filepath.Join(dir, "capture.sock")
	c.QueueBytes = 8 << 20
	c.Redactions = []string{"secret-value"}
	delivered := make(chan Observation, len(callbacks)+2)
	listener, err := net.Listen("unix", c.Socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			w.WriteHeader(http.StatusOK)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var o Observation
		if err := json.Unmarshal(raw, &o); err != nil {
			t.Error(err)
			return
		}
		delivered <- o
		json.NewEncoder(w).Encode(map[string]string{"identity": o.Identity(), "digest": Digest(raw)})
	})}
	defer server.Close()
	go server.Serve(listener)
	e := NewEngine(c)
	defer e.Close()
	headers := testHeaders()
	headers.Set("X-Meshix-Capture-Route", "POST /v1/responses")
	hook := Hook{RequestID: "synthetic-responses", Headers: headers, SourceFormat: "openai-response", Stream: true}
	observe := func(method string, body string) {
		t.Helper()
		hook.Body, _ = json.Marshal([]byte(body))
		raw, err := json.Marshal(hook)
		if err != nil {
			t.Fatal(err)
		}
		e.Observe(method, raw)
	}
	observe("request.intercept_before", `{"stream":true}`)
	for i, body := range callbacks {
		hook.ChunkIndex = i
		observe("response.intercept_stream_chunk", body)
	}
	hook.Outcome, hook.StatusCode = "success", 200
	observe("request.complete", "")
	var observations []Observation
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case o := <-delivered:
			if o.Sequence != uint64(len(observations)+1) || o.ContentBytes != len(o.Body) || o.ContentSHA256 != Digest(o.Body) {
				t.Fatal("invalid emitted sequence or content identity")
			}
			observations = append(observations, o)
			if o.Kind == "completion" {
				return observations
			}
		case <-timeout.C:
			t.Fatal("completion not delivered")
		}
	}
}

func TestResponsesStockScannerCallbacks(t *testing.T) {
	// CLIProxyAPI b681a1e0 codex_executor_stream.go scans lines, translates
	// them, then handlers_stream.go invokes hooks before responsesSSEFramer.
	const wire = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"synthetic\"}}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":7,\"output_tokens\":3,\"total_tokens\":10}}}\n\n"
	var callbacks []string
	scanner := bufio.NewScanner(strings.NewReader(wire))
	for scanner.Scan() {
		callbacks = append(callbacks, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	observations := captureResponsesCallbacks(t, callbacks)
	var retained []byte
	for _, o := range observations[1:] {
		if o.Gap != "" || o.BodyFraming != "stock_sse_candidate" {
			t.Fatalf("unexpected gap/framing: %s %s", o.Gap, o.BodyFraming)
		}
		if o.Kind == "stream_chunk" && (o.ChunkIndex == nil || o.ObservedBodyBytes != len(callbacks[*o.ChunkIndex])) {
			t.Fatal("callback observation changed")
		}
		if len(o.Body) > 0 && (o.BodyFromSequence == nil || *o.BodyFromSequence != o.Sequence-1) {
			t.Fatal("event callback provenance lost")
		}
		retained = append(retained, o.Body...)
	}
	if bytes.Count(retained, []byte("\n\n")) != 2 || !bytes.Contains(retained, []byte("event: response.completed\ndata: ")) || !bytes.Contains(retained, []byte(`"usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}`)) {
		t.Fatal("complete stock events or terminal usage lost")
	}
}

func TestResponsesFramingFailuresAtHookBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		callbacks []string
		gap       string
	}{
		{"event-only", []string{"event: response.completed"}, "truncated_frame_withheld"},
		{"truncated-json", []string{"event: response.completed", `data: {"response":`}, "truncated_frame_withheld"},
		{"malformed-json", []string{"event: response.completed", "data: {bad}\n\n"}, "invalid_or_unredactable_sse_withheld"},
		{"invalid-event", []string{"event: invalid event", "data: {}\n\n"}, "invalid_or_unredactable_sse_withheld"},
		// Inserting the missing newline must still respect the same frame cap.
		{"frame-limit", []string{"event: x", "data: " + strings.Repeat("x", MaxBody-len("event: x")-len("data: "))}, "redaction_frame_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			found := false
			for _, o := range captureResponsesCallbacks(t, tc.callbacks)[1:] {
				if len(o.Body) != 0 {
					t.Fatal("unsafe frame retained")
				}
				found = found || o.Gap == tc.gap
			}
			if !found {
				t.Fatalf("missing %s", tc.gap)
			}
		})
	}
}

func TestResponsesJSONTransportSplits(t *testing.T) {
	// Field-looking text inside JSON is not a callback line boundary. Try
	// every byte split, including CRLF, UTF-8, escapes and multiline data.
	for _, frame := range []string{
		"event: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"secret-value é data: event: retry: id: : \\\"\"}\r\n\r\n",
		"event: response.completed\ndata: {\n" + "data: \"type\":\"response.completed\",\n" + "data: \"response\":{\"usage\":{\"input_tokens\":7}}}\n\n",
	} {
		whole := FrameRedactor{allowUndelimited: true}
		want, _, gap := whole.Feed([]byte(frame), 1, true, true, []string{"secret-value"})
		if gap != "" || len(want) == 0 {
			t.Fatal("invalid test frame")
		}
		for split := 0; split <= len(frame); split++ {
			r := FrameRedactor{allowUndelimited: true}
			a, _, gapA := r.Feed([]byte(frame[:split]), 1, true, false, []string{"secret-value"})
			b, _, gapB := r.Feed([]byte(frame[split:]), 2, true, true, []string{"secret-value"})
			if gapA != "" || gapB != "" || !bytes.Equal(append(a, b...), want) {
				t.Fatalf("split %d changed content or gap: %q %q", split, gapA, gapB)
			}
		}
	}
}

func TestResponsesChunkedJSONAtHookBoundary(t *testing.T) {
	for _, callbacks := range [][]string{
		{"event: response.output_text.delta", `data: {"type":"response.output_text.delta","delta":"literal `, `data: field"}`},
		{"event: response.output_text.delta", "data: {\n", "data: \"type\":\"response.output_text.delta\",\n", "data: \"delta\":\"literal data: field\"}\n\n"},
	} {
		var retained []byte
		for _, o := range captureResponsesCallbacks(t, callbacks)[1:] {
			if o.Gap != "" {
				t.Fatalf("chunked JSON withheld: %s", o.Gap)
			}
			retained = append(retained, o.Body...)
		}
		if !bytes.Contains(retained, []byte(`"delta":"literal data: field"`)) {
			t.Fatal("chunk boundary changed JSON content")
		}
	}
}

func TestResponsesLineCallbacksProtectSemanticSecrets(t *testing.T) {
	for _, kind := range []string{"response.output_text.delta", "response.function_call_arguments.delta"} {
		t.Run(kind, func(t *testing.T) {
			parts := []string{"secret-", "value"}
			if kind == "response.function_call_arguments.delta" {
				parts = []string{`{"api_`, `key":"unknown-credential"}`}
			}
			var callbacks []string
			for _, part := range parts {
				payload, _ := json.Marshal(map[string]any{"type": kind, "item_id": "synthetic", "delta": part})
				callbacks = append(callbacks, "event: "+kind, "data: "+string(payload))
			}
			var retained []byte
			for _, o := range captureResponsesCallbacks(t, callbacks)[1:] {
				if o.Gap != "" || (o.Sequence <= 3 && len(o.Body) > 0) {
					t.Fatal("ambiguous fragment released or redaction gap")
				}
				retained = append(retained, o.Body...)
			}
			if bytes.Contains(retained, []byte("secret-")) || bytes.Contains(retained, []byte("unknown-credential")) || !bytes.Contains(retained, []byte("[REDACTED]")) {
				t.Fatal("semantic credential fragments leaked or lost")
			}
		})
	}
}
