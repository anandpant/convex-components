package capture

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// This controlled comparison keeps the installed 1024-item/16-MiB queues and
// ingress engine identical. The only competing work is the legacy ACK selection
// versus the pending-only indexed selection. It is an offline causal experiment,
// not a claim about a production provider or exact production arrival timing.
func BenchmarkReceiptAcknowledgementBurst1600000(b *testing.B) {
	dir, err := os.MkdirTemp("/tmp", "capture-ledger-burst-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "events.db")
	db := legacyReceiptDB(b, path, 1600000)
	legacy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			w.WriteHeader(200)
			io.WriteString(w, "{}")
			return
		}
		raw, _ := io.ReadAll(io.LimitReader(r.Body, MaxFrame+1))
		var e Observation
		if json.Unmarshal(raw, &e) != nil || e.Validate() != nil {
			http.Error(w, "invalid", 400)
			return
		}
		tx, err := db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, "busy", 503)
			return
		}
		defer tx.Rollback()
		var digest string
		err = tx.QueryRow("SELECT digest FROM events WHERE identity=?", e.Identity()).Scan(&digest)
		if err == sql.ErrNoRows {
			_, err = tx.Exec("INSERT INTO events(identity,digest,destination,instance,boot,request_id,sequence,kind,payload) VALUES(?,?,?,?,?,?,?,?,?)", e.Identity(), Digest(raw), e.Destination, e.Instance, e.Boot, e.RequestID, e.Sequence, e.Kind, raw)
		}
		if err != nil || tx.Commit() != nil {
			http.Error(w, "write", 503)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"identity": e.Identity(), "digest": Digest(raw)})
	})
	run := func(label string, handler http.Handler, database *sql.DB, table string) {
		socket := filepath.Join(dir, label+".sock")
		listener, err := net.Listen("unix", socket)
		if err != nil {
			b.Fatal(err)
		}
		server := &http.Server{Handler: handler}
		go server.Serve(listener)
		c := testConfig()
		c.Socket = socket
		c.QueueBytes = 16 << 20
		e := NewEngine(c)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var ackNanos atomic.Int64
		go func() {
			defer close(done)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					start := time.Now()
					_, _ = database.ExecContext(ctx, "UPDATE "+table+" SET payload=x'' WHERE batch_id=?", "no-such-batch")
					ackNanos.Add(time.Since(start).Nanoseconds())
				}
			}
		}()
		hook := Hook{RequestID: "burst-call", Headers: testHeaders()}
		raw, _ := json.Marshal(hook)
		e.Observe("request.intercept_before", raw)
		body, _ := json.Marshal([]byte("data: {\"text\":\"bounded burst\"}\n\n"))
		hook.Body = body
		for train := 0; train < 12; train++ {
			start := time.Now()
			for i := 0; i < 400; i++ {
				hook.ChunkIndex = train*400 + i
				raw, _ = json.Marshal(hook)
				e.Observe("response.intercept_stream_chunk", raw)
			}
			time.Sleep(max(0, 100*time.Millisecond-time.Since(start)))
		}
		cancel()
		<-done
		deadline := time.Now().Add(10 * time.Second)
		for e.bytes.Load() > 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		b.ReportMetric(float64(e.dropped.Load()), label+"-drops")
		b.ReportMetric(float64(e.controlLost.Load()), label+"-lost-control")
		b.ReportMetric(float64(ackNanos.Load())/1e6, label+"-ack-block-ms")
		e.Close()
		server.Close()
	}
	run("legacy", legacy, db, "events")
	db.Close()
	o, err := OpenOutbox(path, 2<<30, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer o.Close()
	run("dense", o, o.db, "pending_events")
}
