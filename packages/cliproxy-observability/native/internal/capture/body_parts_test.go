package capture

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type joinedBody struct {
	kind  string
	first uint64
	body  []byte
}

// joinParts reassembles split bodies exactly as a reader must: consecutive
// sequences, exact offsets, one manifest, and the whole-body digest.
func joinParts(t *testing.T, records []Observation) []joinedBody {
	t.Helper()
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
	var out []joinedBody
	var open *BodyPart
	for _, o := range records {
		if err := o.Validate(); err != nil {
			t.Fatal("invalid record", o.Sequence, err)
		}
		p := o.BodyPart
		if open != nil && (p == nil || p.Index != open.Index+1 || p.Count != open.Count || p.TotalSHA256 != open.TotalSHA256 || o.Kind != out[len(out)-1].kind || o.Sequence != out[len(out)-1].first+uint64(p.Index)) {
			t.Fatal("interleaved or reordered body part", o.Sequence)
		}
		if p == nil {
			out = append(out, joinedBody{o.Kind, o.Sequence, o.Body})
			continue
		}
		if p.Index == 0 {
			out = append(out, joinedBody{o.Kind, o.Sequence, nil})
		}
		last := &out[len(out)-1]
		if p.Offset != len(last.body) {
			t.Fatal("part offset mismatch", o.Sequence)
		}
		last.body = append(last.body, o.Body...)
		open = p
		if p.Index == p.Count-1 {
			if len(last.body) != p.TotalBytes || Digest(last.body) != p.TotalSHA256 {
				t.Fatal("reassembled body mismatch", last.first)
			}
			open = nil
		}
	}
	if open != nil {
		t.Fatal("unfinished split body")
	}
	return out
}

func observeHook(t *testing.T, e *Engine, method string, h Hook, body []byte) {
	t.Helper()
	if body != nil {
		h.Body, _ = json.Marshal(body)
	}
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	e.Observe(method, raw)
}

func TestTwelveMegabyteRequestAndAfterAuthFitWhileWriterHeld(t *testing.T) {
	var mu sync.Mutex
	var records []Observation
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	e := contentTestEngine(t, 64<<20, func(o Observation) {
		if o.Kind == "request" && o.RequestID == "small" {
			// A held ACK outlasting the writer's 2 s timeout is retried; every retry waits here.
			select {
			case entered <- struct{}{}:
			default:
			}
			<-gate
		}
		mu.Lock()
		records = append(records, o)
		mu.Unlock()
	})
	request := bytes.Repeat([]byte("q"), 12_000_000)
	after := append(bytes.Clone(request), bytes.Repeat([]byte("z"), 77)...)
	// Hold the only writer so every split body stays queued and charged.
	observeContent(t, e, "request.intercept_before", "small", []byte(`{"stream":true}`), 0)
	<-entered
	observeContent(t, e, "request.intercept_before", "large", request, 0)
	observeContent(t, e, "request.intercept_after", "large", after, 0)
	for i := range 64 {
		observeContent(t, e, "response.intercept_stream_chunk", "small", []byte(fmt.Sprintf("data: %d\n\n", i)), i)
	}
	stats := e.content.snapshot(e.bytes.Load())
	if stats.CapacityRejections != 0 || stats.IncompleteCalls != 0 || stats.PeakChargedBytes > 64<<20 {
		t.Fatal("split bodies exceeded the unchanged pool", stats)
	}
	close(gate)
	observeContent(t, e, "request.complete", "small", nil, 0)
	observeContent(t, e, "request.complete", "large", nil, 0)
	waitContentWithin(t, e, time.Minute)
	mu.Lock()
	defer mu.Unlock()
	var large []Observation
	for _, o := range records {
		if o.RequestID == "large" {
			large = append(large, o)
		}
	}
	bodies := joinParts(t, large)
	if len(bodies) != 3 || !bytes.Equal(bodies[0].body, request) || !bytes.Equal(bodies[1].body, after) || bodies[2].kind != "completion" || len(large) != 12+12+1 {
		t.Fatal("split bodies changed", len(large))
	}
	final := e.content.snapshot(0)
	if final.LostBytes != 0 || final.UnknownLosses != 0 || final.MissedCalls != 0 || final.CommittedBytes != final.ObservedBytes {
		t.Fatal("split capture accounting", final)
	}
}

func TestSplitAdmissionIsAllOrNothingWithoutSequenceHoles(t *testing.T) {
	var mu sync.Mutex
	var records []Observation
	e := contentTestEngine(t, 2<<20, func(o Observation) { mu.Lock(); records = append(records, o); mu.Unlock() })
	body := bytes.Repeat([]byte("b"), MaxBody+1)
	observeContent(t, e, "request.intercept_before", "reject", body, 0)
	observeContent(t, e, "response.intercept_stream_chunk", "reject", []byte("later"), 0)
	observeContent(t, e, "request.complete", "reject", nil, 0)
	waitContent(t, e)
	mu.Lock()
	defer mu.Unlock()
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
	if len(records) != 3 || records[0].Gap != "capture_content_capacity" || records[0].ObservedBodyBytes != MaxBody+1 || records[0].BodyPart != nil || string(records[1].Body) != "later" || records[2].LostContentBytes != MaxBody+1 {
		t.Fatal("rejected split body hid loss or left holes", records)
	}
	for i, o := range records {
		if o.Sequence != uint64(i+1) {
			t.Fatal("sequence hole after rejected split")
		}
	}
	stats := e.content.snapshot(0)
	if stats.LostBytes != MaxBody+1 || stats.CapacityRejections != 1 || e.queuedItems.Load() != 0 {
		t.Fatal("rejected split accounting", stats)
	}
}

// The stock header-init frame carries two base64 request copies the plugin
// never decodes. A frame far above 2 MiB must not mark this or any call.
func TestLargeHookFramesAreReadWithoutCollateralGaps(t *testing.T) {
	var mu sync.Mutex
	var records []Observation
	e := contentTestEngine(t, 64<<20, func(o Observation) { mu.Lock(); records = append(records, o); mu.Unlock() })
	observeContent(t, e, "request.intercept_before", "neighbor", []byte(`{"stream":true}`), 0)
	request := bytes.Repeat([]byte(`{"type":"input_image","image_url":"data:image/png;base64,AAAA"}`), 1_800_000/64)
	observeContent(t, e, "request.intercept_before", "long", request, 0)
	init := map[string]any{"RequestID": "long", "ChunkIndex": -1, "OriginalRequest": request, "RequestBody": request, "Metadata": map[string]string{"selected_auth_id": "auth-1"}}
	raw, _ := json.Marshal(init)
	if len(raw) <= MaxFrame {
		t.Fatal("fixture frame is not oversized", len(raw))
	}
	start := time.Now()
	e.Observe("response.intercept_stream_chunk", raw)
	t.Logf("%d-byte header-init frame decoded in %s", len(raw), time.Since(start))
	observeContent(t, e, "response.intercept_stream_chunk", "long", []byte("data: done\n\n"), 0)
	observeContent(t, e, "response.intercept_stream_chunk", "neighbor", []byte("data: ok\n\n"), 0)
	observeContent(t, e, "request.complete", "long", nil, 0)
	observeContent(t, e, "request.complete", "neighbor", nil, 0)
	waitContentWithin(t, e, time.Minute)
	mu.Lock()
	defer mu.Unlock()
	var long []Observation
	for _, o := range records {
		if o.Gap != "" || o.CaptureIncomplete {
			t.Fatal("large frame created a gap", o.RequestID, o.Gap)
		}
		if o.RequestID == "long" {
			long = append(long, o)
		}
	}
	bodies := joinParts(t, long)
	initialized := false
	for _, o := range long {
		initialized = initialized || (o.Kind == "stream_init" && o.SelectedAuthID == "auth-1")
	}
	if len(bodies) != 4 || !bytes.Equal(bodies[0].body, request) || bodies[1].kind != "stream_init" || !initialized {
		t.Fatal("header-init or split request missing", len(bodies))
	}
	if stats := e.content.snapshot(0); stats.UnknownLosses != 0 || stats.IncompleteCalls != 0 || e.dropped.Load() != 0 {
		t.Fatal("large frame counted as loss", stats)
	}
}

func TestUnreadableCallbackMarksOnlyTheCallItNames(t *testing.T) {
	var mu sync.Mutex
	var records []Observation
	e := contentTestEngine(t, 64<<20, func(o Observation) { mu.Lock(); records = append(records, o); mu.Unlock() })
	for _, id := range []string{"named", "typed", "clean"} {
		observeContent(t, e, "request.intercept_before", id, []byte(`{"stream":true}`), 0)
	}
	e.Observe("plugin.unknown", []byte(`{`))
	e.Observe("response.intercept_stream_chunk", []byte(`{"RequestID":"named","Body":"dGFp`))
	e.Observe("response.intercept_stream_chunk", []byte(`{"Body":"dGFpbA==","ChunkIndex":`))
	e.Observe("response.intercept_stream_chunk", []byte(`{"RequestID":"typed","ChunkIndex":"one","Body":"dGFpbA=="}`))
	e.Observe("request.intercept_before", []byte(`not json`))
	for _, id := range []string{"named", "typed", "clean"} {
		observeContent(t, e, "response.intercept_stream_chunk", id, []byte("exact"), 1)
		observeContent(t, e, "request.complete", id, nil, 0)
	}
	waitContent(t, e)
	mu.Lock()
	defer mu.Unlock()
	terminal := map[string]Observation{}
	gaps := map[string]string{}
	for _, o := range records {
		if o.Kind == "completion" {
			terminal[o.RequestID] = o
		}
		if o.Gap == "unreadable_callback" {
			gaps[o.RequestID] = o.Gap
		}
	}
	if !terminal["named"].CaptureIncomplete || terminal["named"].LostContentBytes != 0 || !terminal["typed"].CaptureIncomplete || terminal["typed"].LostContentBytes != 4 || terminal["clean"].CaptureIncomplete || gaps["clean"] != "" || len(gaps) != 2 {
		t.Fatal("unreadable callback was not attributed to exactly its call", terminal, gaps)
	}
	stats := e.content.snapshot(0)
	if stats.UnknownLosses != 4 || stats.IncompleteCalls != 2 || stats.MissedCalls != 1 || stats.LostBytes != 4 {
		t.Fatal("unreadable accounting", stats)
	}
}

func TestMissedCallCountsItsKnownRequestBytes(t *testing.T) {
	e := contentTestEngine(t, 64<<20, func(Observation) {})
	e.config.MaxActive = 1
	observeContent(t, e, "request.intercept_before", "active", []byte(`{}`), 0)
	observeContent(t, e, "request.intercept_before", "missed", []byte(strings.Repeat("m", 100)), 0)
	observeContent(t, e, "request.complete", "missed", nil, 0)
	observeContent(t, e, "request.complete", "active", nil, 0)
	waitContent(t, e)
	stats := e.content.snapshot(0)
	if stats.MissedCalls != 1 || stats.LostBytes != 100 || stats.UnknownLosses != 1 || stats.ObservedBytes != stats.CommittedBytes+stats.LostBytes {
		t.Fatal("missed call accounting", stats)
	}
}

func TestShutdownDuringSplitBodySeparatesUnconfirmedFromLost(t *testing.T) {
	// Refuse the second part's ACK; its writer retries until the shutdown deadline.
	e := contentTestEngineACK(t, 64<<20, func(o Observation) bool { return o.BodyPart == nil || o.BodyPart.Index != 1 })
	observeContent(t, e, "request.intercept_before", "shutdown-split", []byte(`{"stream":true}`), 0)
	observeContent(t, e, "response.intercept_after", "shutdown-split", bytes.Repeat([]byte("s"), 3*MaxBody+11), 0)
	e.Close()
	stats := e.content.snapshot(e.bytes.Load())
	if stats.CommittedBytes != uint64(MaxBody+15) || stats.UnconfirmedBytes != MaxBody || stats.LostBytes != uint64(MaxBody+11) || e.bytes.Load() != 0 || e.queuedItems.Load() != 0 {
		t.Fatal("shutdown split accounting", stats, e.queuedItems.Load())
	}
}

// End to end through the real exporter: each 1 MiB part fits the unchanged
// 1408 KiB segment and 2 MiB envelope, even with maximal correlation metadata.
func TestSplitBodiesDeliverThroughUnchangedSegmentBounds(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "capture-split-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	outbox, err := OpenOutbox(filepath.Join(dir, "private", "events.db"), 256<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	var mu sync.Mutex
	var delivered []Observation
	maxEnvelope, maxSegment := 0, 0
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var envelope struct {
			SegmentEnvelope
			Operation string `json:"operation"`
		}
		json.Unmarshal(raw, &envelope)
		switch envelope.Operation {
		case "health":
			json.NewEncoder(w).Encode(map[string]any{"ready": true, "destinationId": "dev", "deploymentId": "dev-deployment", "instanceId": "test"})
		case "health_record":
			json.NewEncoder(w).Encode(map[string]any{"committed": true, "digest": Digest(raw), "destinationId": "dev", "deploymentId": "dev-deployment"})
		case "segment":
			content, _ := base64.StdEncoding.DecodeString(envelope.Content)
			mu.Lock()
			maxEnvelope, maxSegment = max(maxEnvelope, len(raw)), max(maxSegment, len(content))
			for _, line := range bytes.Split(bytes.TrimSuffix(content, []byte("\n")), []byte("\n")) {
				var o Observation
				if json.Unmarshal(line, &o) != nil || o.Validate() != nil {
					t.Error("invalid delivered line")
				}
				delivered = append(delivered, o)
			}
			mu.Unlock()
			identity, _ := json.Marshal([]any{envelope.Destination, envelope.Instance, envelope.Boot, envelope.Request, envelope.First, envelope.Through})
			call, _ := json.Marshal([]string{envelope.Destination, envelope.Instance, envelope.Boot, envelope.Request})
			json.NewEncoder(w).Encode(map[string]any{"identity": Digest(identity), "digest": envelope.Digest, "callId": Digest(call), "rawCommitted": true, "destinationId": "dev", "deploymentId": "dev-deployment"})
		}
	}))
	defer receiver.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := outbox.RunDelivery(ctx, DeliveryConfig{Destinations: []Destination{{ID: "dev", Deployment: "dev-deployment", URL: receiver.URL + "/cliproxy/capture/v1", Token: "dedicated-delivery-secret-123", Instance: "test"}}}, receiver.Client())
	c := testConfig()
	c.Socket = filepath.Join(dir, "capture.sock")
	c.QueueBytes = 64 << 20
	listener, err := net.Listen("unix", c.Socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: outbox}
	go server.Serve(listener)
	defer server.Close()
	e := NewEngine(c)
	headers := testHeaders()
	for _, name := range []string{"X-Meshix-Request-Id", "X-Meshix-Run-Id", "X-Meshix-Job-Id", "X-Meshix-Trace-Id", "X-Opencode-Session-Id", "X-Meshix-Root-Execution-Id", "X-Meshix-Operation-Id", "X-Meshix-Step-Id", "X-Meshix-Part-Id", "X-Meshix-Attempt-Id"} {
		headers.Set(name, strings.Repeat("c", 256))
	}
	model := strings.Repeat("m", 256)
	request := bytes.Repeat([]byte("Q"), 5*MaxBody+123)
	hook := Hook{RequestID: "segment-split", Headers: headers, RequestedModel: model, TraceID: strings.Repeat("t", 256), SourceFormat: strings.Repeat("f", 64)}
	observeHook(t, e, "request.intercept_before", hook, request)
	hook.Model = model
	observeHook(t, e, "request.intercept_after", hook, append(bytes.Clone(request), 'A'))
	observeHook(t, e, "response.intercept_after", hook, bytes.Repeat([]byte("R"), 2*MaxBody+1))
	hook.Outcome = "succeeded"
	observeHook(t, e, "request.complete", hook, nil)
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		if n == 6+6+3+1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.Close()
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	bodies := joinParts(t, delivered)
	if len(bodies) != 4 || !bytes.Equal(bodies[0].body, request) || len(bodies[1].body) != len(request)+1 || len(bodies[2].body) != 2*MaxBody+1 || bodies[3].kind != "completion" {
		t.Fatal("delivered split bodies changed", len(delivered))
	}
	if maxSegment > MaxSegment || maxEnvelope > MaxFrame {
		t.Fatal("split part exceeded unchanged delivery bounds", maxSegment, maxEnvelope)
	}
	t.Logf("largest segment %d/%d bytes, envelope %d/%d bytes", maxSegment, MaxSegment, maxEnvelope, MaxFrame)
}
