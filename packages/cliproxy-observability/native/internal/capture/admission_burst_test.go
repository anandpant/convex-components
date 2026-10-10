package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The installed path enables budgets only through RunDelivery. Seed a backlog
// larger than CT101's 17k rows/119MB, deliver at its measured ~50 events/sec,
// and pace six concurrent Sol-like streams at the retained 60-113 chunks/sec.
// This is an offline regression, not production campaign acceptance.
func TestDestinationBudgetBacklogBurst(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "capture-admission-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	o, err := OpenOutbox(filepath.Join(dir, "events.db"), 2<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	tx, err := o.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO pending_events(identity,digest,destination,instance,boot,request_id,sequence,kind,payload,received_at) VALUES(?,?,?,?,?,?,?,?,?,'2026-10-01 00:00:00')")
	if err != nil {
		t.Fatal(err)
	}
	var seededBytes int64
	for i := 1; i <= 25000; i++ {
		e := Observation{SchemaVersion: 1, Destination: "dev", Instance: "test", Boot: "seed", RequestID: "backlog", Sequence: uint64(i), Kind: "stream_chunk", Route: "POST /v1/responses", ObservedAt: "2026-10-01T00:00:00Z", Body: bytes.Repeat([]byte("x"), 4096)}
		e.ContentBytes = len(e.Body)
		e.ContentSHA256 = Digest(e.Body)
		raw, _ := json.Marshal(e)
		seededBytes += int64(len(raw))
		if _, err = stmt.Exec(e.Identity(), Digest(raw), e.Destination, e.Instance, e.Boot, e.RequestID, e.Sequence, e.Kind, raw); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if seededBytes < 119<<20 {
		t.Fatal("backlog smaller than live baseline", seededBytes)
	}
	var delivered atomic.Int64
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var h Health
		json.Unmarshal(raw, &h)
		if h.Operation == "health" {
			json.NewEncoder(w).Encode(map[string]any{"ready": true, "destinationId": "dev", "deploymentId": "offline", "instanceId": "test"})
			return
		}
		if h.Operation == "health_record" {
			json.NewEncoder(w).Encode(map[string]any{"committed": true, "digest": Digest(raw), "destinationId": "dev", "deploymentId": "offline"})
			return
		}
		var b SegmentEnvelope
		if json.Unmarshal(raw, &b) != nil || b.Operation != "segment" || b.First == 0 || b.Through < b.First {
			w.WriteHeader(400)
			return
		}
		n := int64(b.Through - b.First + 1)
		timer := time.NewTimer(time.Duration(n) * time.Second / 50)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		id, _ := json.Marshal([]any{b.Destination, b.Instance, b.Boot, b.Request, b.First, b.Through})
		call, _ := json.Marshal([]string{b.Destination, b.Instance, b.Boot, b.Request})
		delivered.Add(n)
		json.NewEncoder(w).Encode(map[string]any{"identity": Digest(id), "digest": b.Digest, "callId": Digest(call), "rawCommitted": true, "destinationId": "dev", "deploymentId": "offline"})
	}))
	defer receiver.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := o.RunDelivery(ctx, DeliveryConfig{Destinations: []Destination{{ID: "dev", Instance: "test", Deployment: "offline", URL: receiver.URL + "/cliproxy/capture/v1", Token: "offline-test-only-token-123456"}}}, receiver.Client())
	defer func() { cancel(); <-done }()
	if o.destinationBudgets["dev"] != 1<<30 {
		t.Fatal("delivery did not enable real destination budget")
	}
	var latencyMu sync.Mutex
	var latencies []time.Duration
	var failures atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := httptest.NewRecorder()
		o.ServeHTTP(rec, r)
		if r.URL.Path == "/events" {
			latencyMu.Lock()
			latencies = append(latencies, time.Since(start))
			latencyMu.Unlock()
			if rec.Code != 200 {
				failures.Add(1)
			}
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
	})
	c := testConfig()
	c.QueueBytes = 16 << 20
	c.MaxActive = 10
	c.Socket = filepath.Join(dir, "capture.sock")
	listener, err := net.Listen("unix", c.Socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	defer server.Close()
	engine := NewEngine(c)
	defer engine.Close()
	start := time.Now()
	var streams sync.WaitGroup
	var peak atomic.Int64
	for stream := 0; stream < 6; stream++ {
		streams.Add(1)
		go func(stream int) {
			defer streams.Done()
			headers := testHeaders()
			headers.Set("X-Meshix-Capture-Route", "POST /v1/responses")
			body, _ := json.Marshal(bytes.Repeat([]byte("tool-result"), 6554))
			h := Hook{RequestID: fmt.Sprint("sol-", stream), Headers: headers, Body: body, RequestedModel: "gpt-6.1-sol", SourceFormat: "openai-responses"}
			raw, _ := json.Marshal(h)
			engine.Observe("request.intercept_before", raw)
			delta, _ := json.Marshal([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"bounded Sol-like text delta from offline fixture\"}\n\n"))
			h.Body = delta
			for i := 0; i < 600; i++ {
				time.Sleep(max(0, time.Until(start.Add(time.Duration(i+1)*10*time.Millisecond))))
				h.ChunkIndex = i
				raw, _ = json.Marshal(h)
				engine.Observe("response.intercept_stream_chunk", raw)
				for n := int64(len(engine.pipes["dev"].queue)); n > peak.Load(); {
					old := peak.Load()
					if n <= old || peak.CompareAndSwap(old, n) {
						break
					}
				}
			}
			h.Body = nil
			h.Outcome = "succeeded"
			raw, _ = json.Marshal(h)
			engine.Observe("request.complete", raw)
		}(stream)
	}
	streams.Wait()
	deadline := time.Now().Add(15 * time.Second)
	for engine.bytes.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	latencyMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var total time.Duration
	for _, d := range latencies {
		total += d
	}
	n := len(latencies)
	median, p99 := time.Duration(0), time.Duration(0)
	if n > 0 {
		median = latencies[n/2]
		p99 = latencies[(n-1)*99/100]
	}
	latencyMu.Unlock()
	t.Logf("seededRows=25000 seededBytes=%d streams=6 chunksPerStreamPerSecond=100 callbacks=3612 drops=%d lostControl=%d queuePeak=%d admissionRequests=%d admissionMedian=%s admissionP99=%s admissionTotal=%s deliveryEvents=%d elapsed=%s", seededBytes, engine.dropped.Load(), engine.controlLost.Load(), peak.Load(), n, median, p99, total, delivered.Load(), time.Since(start))
	if engine.dropped.Load() != 0 || engine.controlLost.Load() != 0 || engine.bytes.Load() != 0 || failures.Load() != 0 {
		t.Fatalf("capture not lossless under active delivery/backlog: drops=%d lostControl=%d queuedBytes=%d failedAdmission=%d", engine.dropped.Load(), engine.controlLost.Load(), engine.bytes.Load(), failures.Load())
	}
	rows, err := o.db.Query("SELECT payload FROM events WHERE boot=? ORDER BY request_id,sequence", engine.boot)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	originals, completions, requests := 0, 0, 0
	expectedChunk := []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"bounded Sol-like text delta from offline fixture\"}\n\n")
	for rows.Next() {
		var raw []byte
		var event Observation
		if err = rows.Scan(&raw); err != nil || json.Unmarshal(raw, &event) != nil {
			t.Fatal("invalid committed content", err)
		}
		switch event.Kind {
		case "request":
			requests++
			if !bytes.Equal(event.Body, bytes.Repeat([]byte("tool-result"), 6554)) {
				t.Fatal("request changed")
			}
		case "completion":
			completions++
			if event.CaptureIncomplete || event.Gap != "" {
				t.Fatal("incomplete terminal")
			}
		case "stream_chunk":
			offset := 0
			for _, n := range event.StockHookChunkLengths {
				if !bytes.Equal(event.Body[offset:offset+n], expectedChunk) {
					t.Fatal("stream bytes changed")
				}
				offset += n
				originals++
			}
			if offset != len(event.Body) {
				t.Fatal("unaccounted bytes")
			}
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if originals != 3600 || requests != 6 || completions != 6 {
		t.Fatal("content or terminals missing", originals, requests, completions)
	}

	if delivered.Load() < 100 {
		t.Fatal("receiver stand-in never drained")
	}
}
