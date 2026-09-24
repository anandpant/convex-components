package capture

import (
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
func captureHookBodies(t *testing.T, request, afterAuth string, callbacks []string, diagnostics ...string) []Observation {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "capture-sse-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	c := testConfig()
	c.Socket = filepath.Join(dir, "capture.sock")
	c.QueueBytes = 8 << 20
	delivered := make(chan Observation, len(callbacks)+3)
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
		if bytes.Contains(raw, []byte("cookie-transport-only")) || bytes.Contains(raw, []byte("opaque-auth-secret")) {
			t.Error("transport credential metadata persisted")
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
	headers.Set("Cookie", "session=cookie-transport-only")
	hook := Hook{RequestID: "synthetic-responses", Headers: headers, SourceFormat: "openai-response", Stream: true}
	observe := func(method string, body string) {
		t.Helper()
		hook.Body, _ = json.Marshal([]byte(body))
		raw, err := json.Marshal(hook)
		if err != nil {
			t.Fatal(err)
		}
		// The host metadata bag can contain credential material. Only the two
		// explicitly typed selected-auth scalars may enter the observation.
		raw = bytes.Replace(raw, []byte(`"Metadata":{`), []byte(`"Metadata":{"credentials":{"password":"opaque-auth-secret"},`), 1)
		e.Observe(method, raw)
	}
	observe("request.intercept_before", request)
	if afterAuth != "" {
		hook.Model, hook.ToFormat = "executed-model", "openai-response"
		hook.Metadata.SelectedAuthID = json.RawMessage(`"selected-id"`)
		hook.Metadata.SelectedAuthIndex = json.RawMessage(`"selected-index"`)
		observe("request.intercept_after", afterAuth)
	}
	for i, body := range callbacks {
		hook.ChunkIndex = i
		observe("response.intercept_stream_chunk", body)
	}
	hook.Outcome, hook.StatusCode = "succeeded", 200
	hook.Error = "diagnostic token secret text"
	if len(diagnostics) > 0 {
		hook.Error = diagnostics[0]
	}
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
				if !o.ErrorPresent || (len(hook.Error) <= 4096 && o.Error != hook.Error) || (len(hook.Error) > 4096 && (o.Error != "" || len(o.MetadataOmissions) == 0)) {
					t.Fatal("recorded diagnostic changed")
				}
				return observations
			}
		case <-timeout.C:
			t.Fatal("completion not delivered")
		}
	}
}

func TestHookContentFidelity(t *testing.T) {
	request := ` { "stream":true, "token":"ordinary-word", "secret":"user-content", "authorization":"quoted-example", "url":"https://example.test/?token=keep", "nested":"{\"api_key\":\"example\"}", "image":{"type":"image_url","url":"data:image/png;base64,aGVsbG8="} } `
	after := `{"model":"executed-model","tools":[{"name":"secret","arguments":{"password":"example"}}]}`
	callbacks := []string{"event: response.created", `data: {"type":"response.created","response":{"id":"synthetic"}}`, "", "event: response.output_text.delta", `data: {"type":"response.output_text.delta","delta":"secret-"}`, "event: response.output_text.delta", `data: {"type":"response.output_text.delta","delta":"value"}`, `data: {"arguments":"{\"api_"}`, `data: {"arguments":"key\":\"keep\"}"}`, "event: response.completed", `data: {"type":"response.completed","response":{"usage":{"input_tokens":7,"output_tokens":3}}}`}
	observations := captureHookBodies(t, request, after, callbacks)
	if string(observations[0].Body) != request || string(observations[1].Body) != after {
		t.Fatal("request bytes changed")
	}
	if observations[1].Kind != "request_after_auth" || observations[1].ExecutionModel != "executed-model" || observations[1].ExecutionProtocol != "openai-response" || observations[1].SelectedAuthID != "selected-id" || observations[1].SelectedAuthIndex != "selected-index" {
		t.Fatal("after-auth evidence missing")
	}
	for i, o := range observations[2 : len(observations)-1] {
		if string(o.Body) != callbacks[i] || o.BodyFraming != "stock_hook_chunk" || o.ChunkIndex == nil || *o.ChunkIndex != i || o.ObservedBodyBytes != len(callbacks[i]) || o.Gap != "" || o.CapturePolicy != CapturePolicy {
			t.Fatalf("callback %d changed", i)
		}
	}
}

func TestHookRetainsEverySplitAndMalformedBytes(t *testing.T) {
	frame := "event: response.output_text.delta\r\ndata: {\"delta\":\"é token secret data: field\"}\r\n\r\n"
	var callbacks []string
	for split := 0; split <= len(frame); split++ {
		callbacks = append(callbacks, frame[:split], frame[split:])
	}
	callbacks = append(callbacks, "event: response.completed", `data: {"truncated":`, "data: {bad}\n\n", string([]byte{0xff, 0, 0x80}))
	observations := captureHookBodies(t, `{"stream":true}`, "", callbacks)
	for i, o := range observations[1 : len(observations)-1] {
		if !bytes.Equal(o.Body, []byte(callbacks[i])) || o.Gap != "" {
			t.Fatalf("callback %d was rewritten or withheld", i)
		}
	}
}

func TestHookBodyLimitRemainsExplicit(t *testing.T) {
	callbacks := []string{strings.Repeat("x", MaxBody), strings.Repeat("y", MaxBody+1)}
	observations := captureHookBodies(t, `{"stream":true}`, "", callbacks)
	if string(observations[1].Body) != callbacks[0] || len(observations[2].Body) != 0 || observations[2].ObservedBodyBytes != MaxBody+1 || observations[2].Gap != "observation_body_limit" {
		t.Fatal("body bounds or loss evidence changed")
	}
}

func TestCredentialMetadataOmissionPreservesContent(t *testing.T) {
	key := testConfig().Bindings[0].Key
	o := Observation{Model: "model-" + key, TraceID: "trace-" + key, Correlation: map[string]string{"requestId": "exact", "runId": key}, Body: []byte(`{"secret":"content"}`)}
	excludeCredentialMetadata(&o, testConfig().Bindings)
	if o.Model != "" || o.TraceID != "" || o.Correlation["runId"] != "" || o.Correlation["requestId"] != "exact" || len(o.MetadataOmissions) != 2 || string(o.Body) != `{"secret":"content"}` {
		t.Fatal("metadata exclusion changed identity or payload")
	}
	for _, raw := range []string{`42`, `{"secret":"opaque-auth-secret"}`, `["id"]`} {
		if metadataIdentity(json.RawMessage(raw), "selectedAuthId", &o) != "" {
			t.Fatal("untyped auth metadata accepted")
		}
	}
}

func TestCompletionDiagnosticFidelityAndBound(t *testing.T) {
	for _, diagnostic := range []string{"request failed: token=" + testConfig().Bindings[0].Key, strings.Repeat("x", 4097)} {
		observations := captureHookBodies(t, `{"stream":true}`, "", nil, diagnostic)
		o := observations[len(observations)-1]
		if len(diagnostic) <= 4096 && o.Error != diagnostic {
			t.Fatal("private diagnostic content scrubbed")
		}
		if len(diagnostic) > 4096 && (o.Error != "" || o.MetadataOmissions[0] != "error:limit" || o.Gap != "diagnostic_text_limit" || o.ObservedErrorBytes != len(diagnostic)) {
			t.Fatal("diagnostic bound not explicit")
		}
	}
}
