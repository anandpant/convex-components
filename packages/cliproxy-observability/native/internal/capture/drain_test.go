package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func drainEvent(sequence uint64, bodyBytes int) []byte {
	e := Observation{SchemaVersion: 1, PluginVersion: Version, CapturePolicy: "hook-body-v1", BodyFraming: "stock_hook_chunk", Destination: "prod", Instance: "instance", Boot: "boot", RequestID: "request", Sequence: sequence, Kind: "stream_chunk", ObservedAt: "2026-09-28T15:29:23Z", Route: "POST /v1/messages", Revision: "r1", Body: bytes.Repeat([]byte("x"), bodyBytes)}
	e.ContentBytes, e.ContentSHA256 = len(e.Body), Digest(e.Body)
	raw, _ := json.Marshal(e)
	return raw
}

func postDrainEvent(o *Outbox, raw []byte) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	o.ServeHTTP(r, httptest.NewRequest("POST", "/events", bytes.NewReader(raw)))
	return r
}

func waitDrain(t *testing.T, o *Outbox, query string, expected int) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := o.db.QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("delivery did not reach %d: %s", expected, query)
}

func TestDrainAtAdmissionCeilingPreservesRetryAndReclaimsPages(t *testing.T) {
	o, err := OpenOutbox(filepath.Join(t.TempDir(), "private", "events.db"), 4<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	raw := drainEvent(1, 256<<10)
	if res := postDrainEvent(o, raw); res.Code != 200 {
		t.Fatal(res.Body.String())
	}
	var configuredCeiling int
	o.db.QueryRow("PRAGMA max_page_count").Scan(&configuredCeiling)
	// Reproduce a database grown by the old exporter to the event-admission
	// ceiling, without putting fixture-only pressure into the new drain reserve.
	o.db.Exec("PRAGMA max_page_count=1024")
	// Fixture pressure fills the actual SQLite ceiling after admission, like a
	// retained receipt ledger. It is unrelated data that the drain must preserve.
	if _, err = o.db.Exec("CREATE TABLE pressure(payload BLOB); INSERT INTO pressure VALUES (x'')"); err != nil {
		t.Fatal(err)
	}
	pressureBytes := 0
	for _, size := range []int{32 << 10, 4096, 1024, 256} {
		for {
			if _, err = o.db.Exec("INSERT INTO pressure VALUES (zeroblob(?))", size); err != nil {
				break
			}
			pressureBytes += size
		}
	}
	var before, ceiling int
	o.db.QueryRow("PRAGMA page_count").Scan(&before)
	o.db.QueryRow("PRAGMA max_page_count").Scan(&ceiling)
	if before != ceiling {
		t.Fatalf("fixture missed SQLite ceiling: %d / %d", before, ceiling)
	}
	o.db.Exec("PRAGMA max_page_count=" + fmt.Sprint(configuredCeiling))
	if res := postDrainEvent(o, drainEvent(2, 32)); res.Code != 507 {
		t.Fatalf("new admission escaped budget: %d", res.Code)
	}
	if res := postDrainEvent(o, raw); res.Code != 200 {
		t.Fatal("exact duplicate lost its durable receipt")
	}
	var wrong atomic.Bool
	wrong.Store(true)
	var attempts atomic.Int32
	sent := make(chan [2]string, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e SegmentEnvelope
		json.NewDecoder(r.Body).Decode(&e)
		if e.Operation == "health" {
			json.NewEncoder(w).Encode(map[string]any{"ready": true, "destinationId": "prod", "deploymentId": "prod-deployment", "instanceId": "instance"})
			return
		}
		identity, _ := json.Marshal([]any{e.Destination, e.Instance, e.Boot, e.Request, e.First, e.Through})
		id := Digest(identity)
		attempts.Add(1)
		sent <- [2]string{id, e.Digest}
		call, _ := json.Marshal([]string{e.Destination, e.Instance, e.Boot, e.Request})
		digest := e.Digest
		if wrong.Load() {
			digest = "wrong-digest"
		}
		json.NewEncoder(w).Encode(map[string]any{"identity": id, "digest": digest, "callId": Digest(call), "rawCommitted": true, "destinationId": "prod", "deploymentId": "prod-deployment"})
	}))
	defer server.Close()
	d := Destination{ID: "prod", Instance: "instance", Deployment: "prod-deployment", URL: server.URL + "/cliproxy/capture/v1", Token: "dedicated-delivery-secret"}
	ctx, cancel := context.WithCancel(context.Background())
	done := o.RunDelivery(ctx, DeliveryConfig{[]Destination{d}}, server.Client())
	defer func() { cancel(); <-done }()
	waitDrain(t, o, "SELECT count(*) FROM batches WHERE state='quarantined'", 1)
	var retained []byte
	o.db.QueryRow("SELECT payload FROM events WHERE state='pending'").Scan(&retained)
	if !bytes.Equal(retained, raw) {
		t.Fatal("wrong ACK changed pending payload")
	}
	wrong.Store(false)
	resume := httptest.NewRecorder()
	o.ServeHTTP(resume, httptest.NewRequest("POST", "/resume", bytes.NewBufferString(`{"destinationId":"prod"}`)))
	if resume.Code != 200 {
		t.Fatal(resume.Body.String())
	}
	waitDrain(t, o, "SELECT count(*) FROM events WHERE state='delivered' AND length(payload)=0", 1)
	cancel()
	<-done
	// The deferred join is harmless after the closed delivery channel.
	var after, preservedPressure int
	o.db.QueryRow("PRAGMA page_count").Scan(&after)
	o.db.QueryRow("SELECT sum(length(payload)) FROM pressure").Scan(&preservedPressure)
	if after >= before || preservedPressure != pressureBytes || attempts.Load() != 2 {
		t.Fatalf("drain did not reclaim only ACKed pages: %d -> %d, pressure %d/%d, attempts %d", before, after, preservedPressure, pressureBytes, attempts.Load())
	}
	if first, retry := <-sent, <-sent; first != retry {
		t.Fatal("retry changed persisted batch identity or digest")
	}
	if res := postDrainEvent(o, raw); res.Code != 200 {
		t.Fatal("post-drain duplicate was not idempotent")
	}
	entries, _ := os.ReadDir(filepath.Join(filepath.Dir(o.path), "segments"))
	if len(entries) != 0 {
		t.Fatal("ACKed replica was not reclaimed")
	}
}

func TestStartupCompactsSparseReceiptsAndBoundsTerminalHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "events.db")
	o, err := OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Historical input models the live layout: payload UPDATEs leave sparse leaf
	// pages while exact event digests remain durable. No pending row is removed.
	tx, _ := o.db.Begin()
	for i := 1; i <= 1000; i++ {
		raw := drainEvent(uint64(i), 1800)
		var event Observation
		json.Unmarshal(raw, &event)
		if _, err = tx.Exec("INSERT INTO pending_events(identity,digest,destination,instance,boot,request_id,sequence,kind,payload,state) VALUES (?,?,?,?,?,?,?,?,?,'delivered')", event.Identity(), Digest(raw), "prod", "instance", "boot", "request", i, "stream_chunk", raw); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 1400; i++ {
		id := Digest([]byte(fmt.Sprint(i)))
		if _, err = tx.Exec("INSERT INTO batches(identity,digest,path,destination,instance,boot,request_id,first_sequence,through_sequence,bytes,state) VALUES (?,?,?,?,?,?,?,?,?,1,'delivered')", id, id, "/historical/"+id, "prod", "instance", "boot", "historical", i+1, i+1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec("UPDATE pending_events SET payload=x''"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	pending := drainEvent(1001, 1024)
	if res := postDrainEvent(o, pending); res.Code != 200 {
		t.Fatal(res.Body.String())
	}
	// Existing quarantined batch metadata and its exact replica survive startup.
	content := append(append([]byte(nil), pending...), '\n')
	segment := filepath.Join(filepath.Dir(path), "segments", Digest(content)+".ndjson")
	if err = os.WriteFile(segment, content, 0600); err != nil {
		t.Fatal(err)
	}
	identityRaw, _ := json.Marshal([]any{"prod", "instance", "boot", "request", 1001, 1001})
	batchID := Digest(identityRaw)
	if _, err = o.db.Exec("INSERT INTO batches(identity,digest,path,destination,instance,boot,request_id,first_sequence,through_sequence,bytes,state) VALUES (?,?,?,?,?,?,?,?,?,?,'quarantined'); UPDATE pending_events SET batch_id=? WHERE state='pending'", batchID, Digest(content), segment, "prod", "instance", "boot", "request", 1001, 1001, len(content), batchID); err != nil {
		t.Fatal(err)
	}
	var before int
	o.db.QueryRow("PRAGMA page_count").Scan(&before)
	if before*4096 < 4<<20 {
		t.Fatal("fixture did not reach admission ceiling")
	}
	o.Close()
	o, err = OpenOutbox(path, 4<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	var after, historical, receipts, mode int
	o.db.QueryRow("PRAGMA page_count").Scan(&after)
	o.db.QueryRow("PRAGMA auto_vacuum").Scan(&mode)
	o.db.QueryRow("SELECT count(*) FROM batches WHERE state='delivered'").Scan(&historical)
	o.db.QueryRow("SELECT count(*) FROM events WHERE state='delivered' AND length(payload)=0").Scan(&receipts)
	if after >= before || after*4096 >= 4<<20 || historical > 1024 || receipts != 1000 || mode != 2 {
		t.Fatalf("startup lost receipts or failed compaction/retention: pages %d -> %d, history %d, receipts %d, mode %d", before, after, historical, receipts, mode)
	}
	var payload []byte
	var batch, digest string
	o.db.QueryRow("SELECT payload,batch_id,digest FROM events WHERE state='pending'").Scan(&payload, &batch, &digest)
	if !bytes.Equal(payload, pending) || batch != batchID || digest != Digest(pending) {
		t.Fatal("startup changed a pending observation")
	}
	stored, err := os.ReadFile(segment)
	if err != nil || !bytes.Equal(stored, content) {
		t.Fatal("startup changed pending replica")
	}
	var historyState string
	o.db.QueryRow("SELECT state FROM batches WHERE identity=?", batchID).Scan(&historyState)
	if historyState != "quarantined" {
		t.Fatal("startup trimmed unacknowledged metadata")
	}
	if res := postDrainEvent(o, drainEvent(1, 1800)); res.Code != 200 {
		t.Fatal("historical event digest no longer deduplicates")
	}
	if res := postDrainEvent(o, drainEvent(1002, 32)); res.Code != 200 {
		t.Fatalf("compaction did not restore admission: %d %s", res.Code, res.Body.String())
	}
}

func TestDeliveryReplicaBoundsAndFilesystemReserve(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		t.Run(fmt.Sprint("reserved=", reserved), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private", "events.db")
			o, err := OpenOutbox(path, 4<<20, 0)
			if err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= 2; i++ {
				var event Observation
				json.Unmarshal(drainEvent(1, 128), &event)
				event.RequestID = fmt.Sprint("request-", i)
				raw, _ := json.Marshal(event)
				if res := postDrainEvent(o, raw); res.Code != 200 {
					t.Fatal(res.Body.String())
				}
			}
			if reserved {
				o.Close()
				o, err = OpenOutbox(path, 4<<20, 1<<62)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer o.Close()
			var attempts atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var envelope SegmentEnvelope
				json.NewDecoder(r.Body).Decode(&envelope)
				if envelope.Operation == "health" {
					json.NewEncoder(w).Encode(map[string]any{"ready": true, "destinationId": "prod", "deploymentId": "prod-deployment", "instanceId": "instance"})
					return
				}
				attempts.Add(1)
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(503)
			}))
			defer server.Close()
			d := Destination{ID: "prod", Instance: "instance", Deployment: "prod-deployment", URL: server.URL + "/cliproxy/capture/v1", Token: "dedicated-delivery-secret"}
			ctx, cancel := context.WithCancel(context.Background())
			done := o.RunDelivery(ctx, DeliveryConfig{[]Destination{d}}, server.Client())
			defer func() { cancel(); <-done }()
			waitDrain(t, o, "SELECT count(*) FROM delivery_status WHERE destination='prod'", 1)
			if !reserved {
				waitDrain(t, o, "SELECT count(*) FROM batches WHERE attempts=1", 1)
			}
			// Allow two ordinary delivery ticks while the first batch is in backoff.
			time.Sleep(2200 * time.Millisecond)
			cancel()
			<-done
			want := int32(1)
			if reserved {
				want = 0
			}
			entries, err := os.ReadDir(filepath.Join(filepath.Dir(path), "segments"))
			if err != nil || attempts.Load() != want || len(entries) != int(want) {
				t.Fatalf("replica or reserve bound violated: attempts %d, replicas %d, error %v", attempts.Load(), len(entries), err)
			}
			waitDrain(t, o, "SELECT count(*) FROM events WHERE state='pending' AND length(payload)>0", 2)
		})
	}
}
