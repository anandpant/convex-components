package capture

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func deliveryOutbox(t *testing.T) *Outbox {
	t.Helper()
	o, err := OpenOutbox(filepath.Join(t.TempDir(), "private", "events.db"), 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close() })
	return o
}
func admitTestEvent(t *testing.T, o *Outbox, destination string, sequence uint64) {
	t.Helper()
	e := Observation{SchemaVersion: 1, PluginVersion: Version, CapturePolicy: CapturePolicy, BodyFraming: "stock_hook_chunk", Destination: destination, Instance: "instance", Boot: "boot", RequestID: "request", Sequence: sequence, Kind: "stream_chunk", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Route: "POST /v1/messages", Revision: "r1", Body: []byte("data: {\"type\":\"ping\"}\n\n")}
	e.ContentBytes = len(e.Body)
	e.ContentSHA256 = Digest(e.Body)
	raw, _ := json.Marshal(e)
	res := httptest.NewRecorder()
	o.ServeHTTP(res, httptest.NewRequest("POST", "/events", bytes.NewReader(raw)))
	if res.Code != 200 {
		t.Fatalf("admission: %d", res.Code)
	}
}
func TestBatchRestartExactIdentityAndReceipt(t *testing.T) {
	o := deliveryOutbox(t)
	d := Destination{ID: "dev", Instance: "instance"}
	for i := 1; i <= 4; i++ {
		admitTestEvent(t, o, "dev", uint64(i))
	}
	b, err := o.nextBatch(d)
	if err != nil || b == nil {
		t.Fatalf("batch: %v", err)
	}
	if b.First != 1 || b.Through != 4 {
		t.Fatal("wrong boundary")
	}
	original, _ := os.ReadFile(b.Path)
	_, _ = o.db.Exec("UPDATE batches SET state='delivering' WHERE identity=?", b.Identity)
	path := o.path
	o.Close()
	o, err = OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	replayed, err := o.nextBatch(d)
	if err != nil || replayed.Identity != b.Identity || replayed.Digest != b.Digest {
		t.Fatal("restart changed batch")
	}
	body, _ := os.ReadFile(replayed.Path)
	if !bytes.Equal(original, body) {
		t.Fatal("restart changed bytes")
	}
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var envelope SegmentEnvelope
		json.NewDecoder(r.Body).Decode(&envelope)
		if r.Header.Get("Authorization") != "Bearer dedicated-delivery-secret" {
			t.Error("missing dedicated auth")
		}
		call, _ := json.Marshal([]string{envelope.Destination, envelope.Instance, envelope.Boot, envelope.Request})
		json.NewEncoder(w).Encode(map[string]any{"identity": b.Identity, "digest": b.Digest, "callId": Digest(call), "rawCommitted": true, "projectionCommitted": false, "destinationId": d.ID, "deploymentId": d.Deployment})
	}))
	defer server.Close()
	d.URL = server.URL + "/cliproxy/capture/v1"
	d.Token = "dedicated-delivery-secret"
	for i := 0; i < 2; i++ {
		status, _, ok := o.sendBatch(context.Background(), d, *b, server.Client())
		if status != 200 || !ok {
			t.Fatal("raw receipt rejected")
		}
	}
	if calls != 2 {
		t.Fatal("missing retry")
	}
	if err = o.ackBatch(*b); err != nil {
		t.Fatal(err)
	}
	var bytes int
	o.db.QueryRow("SELECT sum(length(payload)) FROM events").Scan(&bytes)
	if bytes != 0 {
		t.Fatal("replicas retained after exact receipt")
	}
	var receipts int
	o.db.QueryRow("SELECT count(*) FROM events WHERE state='delivered'").Scan(&receipts)
	if receipts != 4 {
		t.Fatal("compact receipts lost")
	}
	if _, err = os.Stat(b.Path); !os.IsNotExist(err) {
		t.Fatal("segment replica not reclaimed")
	}
}
func TestRejectedReceiptRetainsPayload(t *testing.T) {
	o := deliveryOutbox(t)
	admitTestEvent(t, o, "dev", 1)
	d := Destination{ID: "dev", Instance: "instance"}
	b, _ := o.nextBatch(d)
	for _, mode := range []string{"wrong_digest", "missing_raw_commit", "oversized", "unauthorized"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "unauthorized" {
					w.WriteHeader(401)
					return
				}
				if mode == "oversized" {
					w.Write(make([]byte, 4097))
					return
				}
				call, _ := json.Marshal([]string{b.Destination, b.Instance, b.Boot, b.Request})
				digest := b.Digest
				if mode == "wrong_digest" {
					digest = "wrong"
				}
				json.NewEncoder(w).Encode(map[string]any{"identity": b.Identity, "digest": digest, "callId": Digest(call), "rawCommitted": mode != "missing_raw_commit"})
			}))
			defer server.Close()
			d.URL = server.URL
			_, _, ok := o.sendBatch(context.Background(), d, *b, server.Client())
			if ok {
				t.Fatal("accepted bad receipt")
			}
			if _, err := os.Stat(b.Path); err != nil {
				t.Fatal("lost pending content")
			}
		})
	}
}
func TestDestinationPinAndResegmentation(t *testing.T) {
	o := deliveryOutbox(t)
	d := Destination{ID: "dev", Instance: "instance", Deployment: "dev-deployment", URL: "https://dev.example/cliproxy/capture/v1", Token: "dedicated-delivery-secret"}
	if err := (DeliveryConfig{[]Destination{d}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if o.bindDestination(d) != nil {
		t.Fatal("initial binding")
	}
	changed := d
	changed.URL = "https://prod.example/cliproxy/capture/v1"
	if o.bindDestination(changed) == nil {
		t.Fatal("retargeted queued identity")
	}
	for i := 1; i <= 4; i++ {
		admitTestEvent(t, o, "dev", uint64(i))
	}
	b, _ := o.nextBatch(d)
	if err := o.resegment(*b); err != nil {
		t.Fatal(err)
	}
	a, _ := o.nextBatch(d)
	if a.First != 1 || a.Through != 2 || a.Identity == b.Identity {
		t.Fatal("incorrect smaller immutable segment")
	}
	o.db.Exec("UPDATE batches SET state='delivered' WHERE identity=?", a.Identity)
	next, _ := o.nextBatch(d)
	if next.First != 3 || next.Through != 4 {
		t.Fatal("missing tail")
	}
}
func TestFairDeliveryAndContentFreeHealth(t *testing.T) {
	o := deliveryOutbox(t)
	admitTestEvent(t, o, "dev", 1)
	admitTestEvent(t, o, "prod", 1)
	var devCalls, prodCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&raw)
		var dest, op string
		json.Unmarshal(raw["destinationId"], &dest)
		json.Unmarshal(raw["operation"], &op)
		if op == "health" {
			if _, ok := raw["contentBase64"]; ok {
				t.Error("health carried content")
			}
			json.NewEncoder(w).Encode(map[string]any{"ready": true, "destinationId": dest, "deploymentId": dest + "-deployment", "instanceId": "instance"})
			return
		}
		if dest == "dev" {
			devCalls.Add(1)
			w.WriteHeader(401)
			return
		}
		prodCalls.Add(1)
		b, _ := json.Marshal(raw)
		var env SegmentEnvelope
		json.Unmarshal(b, &env)
		id, _ := json.Marshal([]any{dest, env.Instance, env.Boot, env.Request, env.First, env.Through})
		call, _ := json.Marshal([]string{dest, env.Instance, env.Boot, env.Request})
		json.NewEncoder(w).Encode(map[string]any{"identity": Digest(id), "digest": env.Digest, "callId": Digest(call), "rawCommitted": true, "destinationId": dest, "deploymentId": dest + "-deployment"})
	}))
	defer server.Close()
	c := DeliveryConfig{}
	for _, dest := range []string{"dev", "prod"} {
		c.Destinations = append(c.Destinations, Destination{ID: dest, Instance: "instance", Deployment: dest + "-deployment", URL: server.URL + "/cliproxy/capture/v1", Token: "dedicated-delivery-secret"})
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := o.RunDelivery(ctx, c, server.Client())
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var ready int
		o.db.QueryRow("SELECT count(*) FROM batches WHERE (destination='dev' AND state='quarantined') OR (destination='prod' AND state='delivered')").Scan(&ready)
		if ready == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if devCalls.Load() != 1 || prodCalls.Load() != 1 {
		t.Fatalf("fairness: dev %d prod %d", devCalls.Load(), prodCalls.Load())
	}
	var state string
	o.db.QueryRow("SELECT state FROM batches WHERE destination='dev'").Scan(&state)
	if state != "quarantined" {
		t.Fatal("auth rejection not quarantined")
	}
	o.db.QueryRow("SELECT state FROM batches WHERE destination='prod'").Scan(&state)
	if state != "delivered" {
		t.Fatal("healthy destination blocked")
	}
}
func TestOrphanRecovery(t *testing.T) {
	o := deliveryOutbox(t)
	dir := filepath.Join(filepath.Dir(o.path), "segments")
	path := filepath.Join(dir, Digest([]byte("orphan"))+".ndjson")
	if err := durableSegment(path, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if err := o.reconcileSegments(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("orphan not reconciled")
	}
}
func TestDeliveryNeverFollowsRedirects(t *testing.T) {
	o := deliveryOutbox(t)
	var followed atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cliproxy/capture/v1" {
			w.Header().Set("Location", "/wrong-destination")
			w.WriteHeader(302)
			return
		}
		followed.Add(1)
		w.WriteHeader(200)
	}))
	defer server.Close()
	d := Destination{ID: "dev", Instance: "instance", Deployment: "dev-deployment", URL: server.URL + "/cliproxy/capture/v1", Token: "dedicated-delivery-secret"}
	ctx, cancel := context.WithCancel(context.Background())
	done := o.RunDelivery(ctx, DeliveryConfig{[]Destination{d}}, server.Client())
	deadline := time.Now().Add(2 * time.Second)
	var state string
	for time.Now().Before(deadline) {
		o.db.QueryRow("SELECT state FROM delivery_status WHERE destination='dev'").Scan(&state)
		if state == "quarantined" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if followed.Load() != 0 || state != "quarantined" {
		t.Fatal("redirect escaped pinned recipient or failed to quarantine")
	}
}

func TestLegacyOutboxMigratesAndReclaimsPayloadPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "events.db")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	old, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = old.Exec("CREATE TABLE legacy_payload (id INTEGER PRIMARY KEY,payload BLOB); INSERT INTO legacy_payload VALUES (1,zeroblob(1048576));"); err != nil {
		t.Fatal(err)
	}
	old.Close()
	o, err := OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	var size, mode, before, after int
	o.db.QueryRow("SELECT length(payload) FROM legacy_payload WHERE id=1").Scan(&size)
	o.db.QueryRow("PRAGMA auto_vacuum").Scan(&mode)
	if size != 1048576 || mode != 2 {
		t.Fatalf("migration lost data or did not enable incremental vacuum: size=%d mode=%d", size, mode)
	}
	o.db.QueryRow("PRAGMA page_count").Scan(&before)
	if _, err = o.db.Exec("DELETE FROM legacy_payload; PRAGMA incremental_vacuum"); err != nil {
		t.Fatal(err)
	}
	o.db.QueryRow("PRAGMA page_count").Scan(&after)
	if after >= before {
		t.Fatalf("payload pages not reclaimed: %d -> %d", before, after)
	}
}
func TestMinimumOutboxBudgetCanDeliver(t *testing.T) {
	o, err := OpenOutbox(filepath.Join(t.TempDir(), "private", "events.db"), 4<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	admitTestEvent(t, o, "dev", 1)
	b, err := o.nextBatch(Destination{ID: "dev", Instance: "instance"})
	if err != nil || b == nil {
		t.Fatalf("accepted minimum budget cannot deliver: %v", err)
	}
	if err := o.ackBatch(*b); err != nil {
		t.Fatal(err)
	}
}
