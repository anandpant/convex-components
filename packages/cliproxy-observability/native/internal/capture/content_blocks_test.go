package capture

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func contentTestEngine(t *testing.T, budget int, handler func(Observation)) *Engine {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "capture-block-")
	if err != nil {
		t.Fatal(err)
	}
	c := testConfig()
	c.Socket = filepath.Join(dir, "capture.sock")
	c.QueueBytes = budget
	listener, err := net.Listen("unix", c.Socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			w.WriteHeader(200)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var o Observation
		if json.Unmarshal(raw, &o) != nil || o.Validate() != nil {
			t.Error("invalid event")
			w.WriteHeader(400)
			return
		}
		handler(o)
		json.NewEncoder(w).Encode(map[string]string{"identity": o.Identity(), "digest": Digest(raw)})
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close(); os.RemoveAll(dir) })
	e := NewEngine(c)
	t.Cleanup(e.Close)
	return e
}
func observeContent(t *testing.T, e *Engine, method, request string, body []byte, index int) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	h := Hook{RequestID: request, Headers: testHeaders(), Body: encoded, ChunkIndex: index, Outcome: "succeeded"}
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	e.Observe(method, raw)
}
func waitContent(t *testing.T, e *Engine) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for e.bytes.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.bytes.Load() != 0 {
		t.Fatal("content did not reach durable ACK", e.Status())
	}
}

func TestContentTimerFlushesContinuousTrafficByBlockAge(t *testing.T) {
	delivered := make(chan Observation, 10)
	e := contentTestEngine(t, 64<<20, func(o Observation) { delivered <- o })
	observeContent(t, e, "request.intercept_before", "timer", []byte(`{"stream":true}`), 0)
	start := time.Now()
	observeContent(t, e, "response.intercept_stream_chunk", "timer", []byte("first"), 0)
	for i := 1; i <= 8; i++ {
		time.Sleep(100 * time.Millisecond)
		observeContent(t, e, "response.intercept_stream_chunk", "timer", []byte("later"), i)
	}
	deadline := time.NewTimer(400 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case o := <-delivered:
			if o.Kind != "stream_chunk" {
				continue
			}
			if time.Since(start) > 1200*time.Millisecond || !bytes.HasPrefix(o.Body, []byte("first")) || len(o.StockHookChunkLengths) != 9 {
				t.Fatal("timer waited for idle or changed content")
			}
			observeContent(t, e, "request.complete", "timer", nil, 0)
			waitContent(t, e)
			return
		case <-deadline.C:
			t.Fatal("continuous stream never flushed")
		}
	}
}
func TestContentCapacityLossKeepsLaterBytesAndExactLoss(t *testing.T) {
	var mu sync.Mutex
	var records []Observation
	gate := make(chan struct{})
	entered := make(chan struct{})
	var gateOnce sync.Once
	unblock := func() { gateOnce.Do(func() { close(gate) }) }
	defer unblock()
	e := contentTestEngine(t, 2<<20, func(o Observation) {
		if o.Kind == "request" {
			close(entered)
			<-gate
		}
		mu.Lock()
		records = append(records, o)
		mu.Unlock()
	})
	request := []byte(`{"stream":true}`)
	observeContent(t, e, "request.intercept_before", "gap", request, 0)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not enter blocked ACK")
	}
	start := time.Now()
	observeContent(t, e, "response.intercept_stream_chunk", "gap", bytes.Repeat([]byte("x"), MaxBody), 0)
	t.Logf("1MiB callback decode+admission took %s (race/emulation timing, not a provider latency gate)", time.Since(start))
	// This returned while the normal writer is still blocked on its prior ACK.
	unblock()
	tail := []byte("exact tail after gap")
	observeContent(t, e, "response.intercept_stream_chunk", "gap", tail, 1)
	observeContent(t, e, "request.complete", "gap", nil, 0)
	waitContent(t, e)
	mu.Lock()
	defer mu.Unlock()
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
	if len(records) != 4 || records[1].Gap != "capture_content_capacity" || !bytes.Equal(records[2].Body, tail) || records[3].Kind != "completion" || records[3].LostContentBytes != MaxBody || !records[3].CaptureIncomplete {
		t.Fatal("loss hid later content or terminal", records)
	}
	stats := e.content.snapshot(0)
	if stats.LostBytes != MaxBody || stats.ObservedBytes != uint64(MaxBody+len(request)+len(tail)) || stats.CommittedBytes != uint64(len(request)+len(tail)) || stats.IncompleteCalls != 1 || stats.CapacityRejections == 0 || stats.PeakChargedBytes > 2<<20 {
		t.Fatal("incorrect byte accounting", stats)
	}
}
func TestContentShutdownFlushesExactPrefixWithoutInventingOutcome(t *testing.T) {
	var mu sync.Mutex
	var records []Observation
	e := contentTestEngine(t, 64<<20, func(o Observation) { mu.Lock(); records = append(records, o); mu.Unlock() })
	observeContent(t, e, "request.intercept_before", "shutdown", []byte(`{"stream":true}`), 0)
	observeContent(t, e, "response.intercept_stream_chunk", "shutdown", []byte{0xff, 0, 0xc3}, 0)
	observeContent(t, e, "response.intercept_stream_chunk", "shutdown", []byte{0xa9}, 1)
	e.Close()
	e.Close()
	mu.Lock()
	defer mu.Unlock()
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
	if len(records) != 3 || !bytes.Equal(records[1].Body, []byte{0xff, 0, 0xc3, 0xa9}) || records[2].Kind != "completion" || records[2].Outcome != "" || records[2].Gap != "capture_shutdown_incomplete" || !records[2].CaptureIncomplete {
		t.Fatal("shutdown lost prefix or invented provider result", records)
	}
	if e.bytes.Load() != 0 || e.pipes["dev"].items.Load() != 0 || e.pipes["dev"].controlItems.Load() != 0 || e.content.lost.Load() != 0 || e.content.unconfirmed.Load() != 0 {
		t.Fatal("shutdown retained charges or falsely lost committed bytes", e.Status())
	}
}
func TestContentFrameAndSizeFlushPreserveAllBoundaries(t *testing.T) {
	callbacks := make([]string, 1025)
	for i := range callbacks {
		callbacks[i] = "x"
	}
	callbacks = append(callbacks, string(bytes.Repeat([]byte("y"), streamBlockBytes)), "tail")
	records := captureHookBodies(t, `{"stream":true}`, "", callbacks)
	assertHookChunks(t, records[1:len(records)-1], callbacks)
	blocks := records[1 : len(records)-1]
	if len(blocks) != 4 || len(blocks[0].StockHookChunkLengths) != 1024 || len(blocks[2].Body) != streamBlockBytes {
		t.Fatal("flush bounds not applied", len(blocks))
	}
}

func TestUnattributedRPCGapIsExplicitButUnknownMethodsAreIgnored(t *testing.T) {
	var mu sync.Mutex
	var records []Observation
	e := contentTestEngine(t, 64<<20, func(o Observation) { mu.Lock(); records = append(records, o); mu.Unlock() })
	observeContent(t, e, "request.intercept_before", "rpc-gap", []byte(`{"stream":true}`), 0)
	e.Observe("plugin.unknown", nil)
	e.RecordGap("plugin.unknown")
	if e.content.unknownLoss.Load() != 0 {
		t.Fatal("unknown method changed capture coverage")
	}
	e.RecordGap("response.intercept_stream_chunk")
	observeContent(t, e, "response.intercept_stream_chunk", "rpc-gap", []byte("tail"), 1)
	observeContent(t, e, "request.complete", "rpc-gap", nil, 0)
	waitContent(t, e)
	mu.Lock()
	defer mu.Unlock()
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
	last := records[len(records)-1]
	if len(records) != 4 || records[1].Gap != "unattributed_rpc_gap" || string(records[2].Body) != "tail" || !last.CaptureIncomplete || last.LostContentBytes != 0 || e.content.unknownLoss.Load() != 1 {
		t.Fatal("unknown gap claimed complete content", records, e.Status())
	}
}
