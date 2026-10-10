package capture

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWorkerGroupACKLossRetainsExactRecords(t *testing.T) {
	for _, fault := range []string{"digest", "trailing", "partial", "oversized"} {
		t.Run(fault, func(t *testing.T) {
			o := deliveryOutbox(t)
			c := testConfig()
			dir, err := os.MkdirTemp("/tmp", "capture-batch-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			c.Socket = filepath.Join(dir, "capture.sock")
			l, err := net.Listen("unix", c.Socket)
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var firstBatch []byte
			var batchRequests int
			wrongACK := make(chan struct{})
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(raw))
				rec := httptest.NewRecorder()
				o.ServeHTTP(rec, r)
				if r.URL.Path == "/events/batch" && rec.Code == 200 {
					mu.Lock()
					batchRequests++
					n := batchRequests
					if n == 1 {
						firstBatch = append([]byte(nil), raw...)
					} else if n == 2 && !bytes.Equal(firstBatch, raw) {
						t.Error("unknown ACK retry changed serialized identities")
					}
					mu.Unlock()
					if n == 1 {
						var acks []eventACK
						json.Unmarshal(rec.Body.Bytes(), &acks)
						switch fault {
						case "digest":
							acks[len(acks)-1].Digest = "wrong"
						case "partial":
							acks = acks[:len(acks)-1]
						}
						json.NewEncoder(w).Encode(acks)
						if fault == "trailing" {
							io.WriteString(w, "invalid")
						}
						if fault == "oversized" {
							w.Write(bytes.Repeat([]byte(" "), maxAdmissionEvents*256))
						}
						close(wrongACK)
						return
					}
				}
				w.WriteHeader(rec.Code)
				w.Write(rec.Body.Bytes())
			})}
			go server.Serve(l)
			defer server.Close()
			e := NewEngine(c)
			defer e.Close()
			// Pre-populate the existing queue, avoiding timing-dependent producer bursts.
			for i := uint64(1); i <= 128; i++ {
				var event Observation
				json.Unmarshal(batchEvent(i), &event)
				q := queued{event, len(event.Body) + 8192}
				e.bytes.Add(int64(q.size))
				e.pipes["dev"].bytes.Add(int64(q.size))
				e.pipes["dev"].items.Add(1)
				e.pipes["dev"].queue <- q
			}
			select {
			case <-wrongACK:
			case <-time.After(5 * time.Second):
				t.Fatal("no grouped admission")
			}
			if e.bytes.Load() == 0 {
				t.Fatal("wrong per-record ACK released queued data")
			}
			deadline := time.Now().Add(5 * time.Second)
			for e.bytes.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if e.bytes.Load() != 0 || e.dropped.Load() != 0 || e.controlLost.Load() != 0 {
				t.Fatal("durable group did not finish without losses")
			}
			var rows int
			o.db.QueryRow("SELECT pending_rows FROM outbox_counts WHERE id=1").Scan(&rows)
			if rows != 128 || e.admission.accepted.Load() != 128 || e.admission.otherFailures.Load() == 0 {
				t.Fatal("lost ACK duplicated, lost or falsely acknowledged records", rows, e.admission.snapshot())
			}
		})
	}
}

func TestHeldGroupsStayInsideExistingItemAllowances(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "capture-item-bound-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	c := testConfig()
	c.Socket, c.QueueBytes = filepath.Join(dir, "unavailable.sock"), 16<<20
	e := NewEngine(c)
	defer e.Close()
	h := Hook{RequestID: "bounded-held-group", Headers: testHeaders()}
	raw, _ := json.Marshal(h)
	e.Observe("request.intercept_before", raw)
	h.Body, _ = json.Marshal([]byte("bounded stream content"))
	for i := 0; i < 2048; i++ {
		h.ChunkIndex = i
		raw, _ = json.Marshal(h)
		e.Observe("response.intercept_stream_chunk", raw)
	}
	p := e.pipes["dev"]
	if p.items.Load() != 1024 || p.controlItems.Load() != 128 || e.dropped.Load() != 1025 || e.controlLost.Load() != 897 {
		t.Fatal("held groups enlarged or bypassed the existing limits", p.items.Load(), p.controlItems.Load(), e.dropped.Load(), e.controlLost.Load())
	}
}
