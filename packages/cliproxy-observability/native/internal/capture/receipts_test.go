package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func eventFingerprint(t testing.TB, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query("SELECT identity,digest,destination,instance,boot,request_id,sequence,kind,payload,received_at,state,batch_id FROM events ORDER BY identity")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	h := sha256.New()
	for rows.Next() {
		var a, b, c, d, e, f, k, received, state string
		var seq int64
		var payload []byte
		var batch sql.NullString
		if err = rows.Scan(&a, &b, &c, &d, &e, &f, &seq, &k, &payload, &received, &state, &batch); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal([]any{a, b, c, d, e, f, seq, k, payload, received, state, batch})
		h.Write(raw)
		h.Write([]byte{'\n'})
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func legacyReceiptDB(t testing.TB, path string, n int) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE events(identity TEXT PRIMARY KEY,digest TEXT NOT NULL,destination TEXT NOT NULL,instance TEXT NOT NULL,boot TEXT NOT NULL,request_id TEXT NOT NULL,sequence INTEGER NOT NULL,kind TEXT NOT NULL,payload BLOB NOT NULL,received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,state TEXT NOT NULL DEFAULT 'pending',batch_id TEXT);CREATE INDEX events_call ON events(destination,instance,boot,request_id,sequence);CREATE INDEX events_pending ON events(destination,state,received_at);`)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	tx, _ := db.Begin()
	stmt, err := tx.Prepare("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,x'','2026-10-07 20:53:37','delivered',?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		id := Digest([]byte(fmt.Sprint(i)))
		digest := Digest([]byte(fmt.Sprint(i, "content")))
		request := fmt.Sprintf("%08x-932e-463e-bc16-117df4a98f28", i/1600)
		batch := Digest([]byte(fmt.Sprint(i/256, "batch")))
		if _, err = stmt.Exec(id, digest, "meshix-prod", "cliproxy-ct101", "640c0db0361135e58189071594eec650", request, i%1600+1, "stream_chunk", batch); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestLegacyReceiptsPreserveEveryLogicalColumnAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	db := legacyReceiptDB(t, path, 500)
	raw := drainEvent(800, 1024)
	var event Observation
	json.Unmarshal(raw, &event)
	_, err := db.Exec("INSERT INTO events(identity,digest,destination,instance,boot,request_id,sequence,kind,payload,state,batch_id) VALUES (?,?,?,?,?,?,?,?,?,'pending','quarantined-batch')", event.Identity(), Digest(raw), event.Destination, event.Instance, event.Boot, event.RequestID, event.Sequence, event.Kind, raw)
	if err != nil {
		t.Fatal(err)
	}
	before := eventFingerprint(t, db)
	db.Close()
	o, err := OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if after := eventFingerprint(t, o.db); after != before {
		t.Fatal("migration changed a logical row")
	}
	if res := postDrainEvent(o, raw); res.Code != 200 {
		t.Fatal("pending replay lost")
	}
	event.Body = []byte("changed")
	event.ContentBytes = len(event.Body)
	event.ContentSHA256 = Digest(event.Body)
	changed, _ := json.Marshal(event)
	if res := postDrainEvent(o, changed); res.Code != 409 {
		t.Fatalf("identity conflict: %d", res.Code)
	}
	o.Close()
	o, err = OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if after := eventFingerprint(t, o.db); after != before {
		t.Fatal("reopen changed a logical row")
	}
}

func TestReceiptMoveRollsBackAsOneTransaction(t *testing.T) {
	o := deliveryOutbox(t)
	admitTestEvent(t, o, "dev", 1)
	b, err := o.nextBatch(Destination{ID: "dev", Instance: "instance"})
	if err != nil {
		t.Fatal(err)
	}
	before := eventFingerprint(t, o.db)
	tx, _ := o.db.Begin()
	if err = moveReceipts(tx, "batch_id=?", b.Identity); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	if after := eventFingerprint(t, o.db); after != before {
		t.Fatal("uncommitted receipt move lost pending data")
	}
	if err = o.ackBatch(*b); err != nil {
		t.Fatal(err)
	}
	var n int
	o.db.QueryRow("SELECT count(*) FROM acknowledged_events").Scan(&n)
	if n != 1 {
		t.Fatal("ACK receipt missing")
	}
	// Replays remain exact after durable ACK and payload clearing.
	e := Observation{SchemaVersion: 1, PluginVersion: Version, CapturePolicy: "hook-body-v1", BodyFraming: "stock_hook_chunk", Destination: "dev", Instance: "instance", Boot: "boot", RequestID: "request", Sequence: 1, Kind: "stream_chunk", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Route: "POST /v1/messages", Revision: "r1", Body: []byte("different")}
	e.ContentBytes = len(e.Body)
	e.ContentSHA256 = Digest(e.Body)
	raw, _ := json.Marshal(e)
	if res := postDrainEvent(o, raw); res.Code != 409 {
		t.Fatal("ACK receipt conflict lost")
	}
}

func BenchmarkReceiptLedger1600000(b *testing.B) {
	const n = 1600000
	path := filepath.Join(b.TempDir(), "events.db")
	db := legacyReceiptDB(b, path, n)
	before := eventFingerprint(b, db)
	var oldPages int64
	db.QueryRow("PRAGMA page_count").Scan(&oldPages)
	start := time.Now()
	_, err := db.Exec("UPDATE events SET state='delivered',payload=x'' WHERE batch_id=?", "nonexistent")
	if err != nil {
		b.Fatal(err)
	}
	legacyMicros := float64(time.Since(start).Microseconds())
	db.Close()
	start = time.Now()
	o, err := OpenOutbox(path, 2<<30, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer o.Close()
	migrationSeconds := time.Since(start).Seconds()
	if after := eventFingerprint(b, o.db); after != before {
		b.Fatal("large migration changed historical rows")
	}
	// Repack the one-time migration to compare equally compact physical layouts.
	if _, err = o.db.Exec("VACUUM"); err != nil {
		b.Fatal(err)
	}
	var pages int64
	o.db.QueryRow("PRAGMA page_count").Scan(&pages)
	oldSize := float64(oldPages*4096) / n
	newSize := float64(pages*4096) / n
	start = time.Now()
	_, err = o.db.Exec("UPDATE pending_events SET batch_id=NULL WHERE batch_id=?", "nonexistent")
	if err != nil {
		b.Fatal(err)
	}
	lookupMicros := float64(time.Since(start).Microseconds())
	event := Observation{SchemaVersion: 1, PluginVersion: Version, CapturePolicy: "hook-body-v1", Destination: "prod", Instance: "instance", Boot: "boot", RequestID: "new-request", Kind: "stream_chunk", BodyFraming: "stock_hook_chunk", Route: "POST /v1/messages", Revision: "r1", Body: []byte("data: test\n\n")}
	event.ContentBytes = len(event.Body)
	event.ContentSHA256 = Digest(event.Body)
	b.ResetTimer()
	for i := 0; i < 256*b.N; i++ {
		event.Sequence = uint64(i + 1)
		event.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
		raw, _ := json.Marshal(event)
		res := httptest.NewRecorder()
		o.ServeHTTP(res, httptest.NewRequest("POST", "/events", bytes.NewReader(raw)))
		if res.Code != 200 {
			b.Fatal(res.Body.String())
		}
	}
	b.StopTimer()
	b.ReportMetric(legacyMicros, "legacy-ack-scan-us")
	b.ReportMetric(migrationSeconds, "migration-s")
	b.ReportMetric(oldSize, "legacy-bytes/receipt")
	b.ReportMetric(newSize, "dense-bytes/receipt")
	b.ReportMetric(lookupMicros, "pending-ack-lookup-us")
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(256*b.N), "admission-us/event")
	batch, err := o.nextBatch(Destination{ID: "prod", Instance: "instance"})
	if err != nil || batch == nil {
		b.Fatal(err)
	}
	start = time.Now()
	if err = o.ackBatch(*batch); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(time.Since(start).Microseconds()), "exact-ack-us")
	_, _ = o.db.ExecContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)")
}

func TestLegacyRestoreRetainsPostMigrationObservations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	db := legacyReceiptDB(t, path, 500)
	db.Close()
	o, err := OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw := drainEvent(800, 1024)
	if res := postDrainEvent(o, raw); res.Code != 200 {
		t.Fatal(res.Body.String())
	}
	batch, err := o.nextBatch(Destination{ID: "prod", Instance: "instance"})
	if err != nil {
		t.Fatal(err)
	}
	if err = o.ackBatch(*batch); err != nil {
		t.Fatal(err)
	}
	pending := drainEvent(801, 2048)
	if res := postDrainEvent(o, pending); res.Code != 200 {
		t.Fatal(res.Body.String())
	}
	before := eventFingerprint(t, o.db)
	o.Close()
	if err = RestoreLegacyOutbox(path, 64<<20, 0); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if after := eventFingerprint(t, db); after != before {
		t.Fatal("legacy restore discarded or changed current observations")
	}
	// Legacy acknowledgement SQL must be writable, rather than targeting a view.
	if _, err = db.Exec("UPDATE events SET state=state WHERE batch_id IS NULL"); err != nil {
		t.Fatal("prior exporter cannot use the restored ledger")
	}
	db.Close()
	o, err = OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if after := eventFingerprint(t, o.db); after != before {
		t.Fatal("forward migration after rollback changed records")
	}
}

func TestMigrationFitsExistingCeilingWithDistinctReceiptScopes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	db := legacyReceiptDB(t, path, 6500)
	if _, err := db.Exec("UPDATE events SET request_id=identity,batch_id=identity"); err != nil {
		t.Fatal(err)
	}
	before := eventFingerprint(t, db)
	db.Close()
	o, err := OpenOutbox(path, 4<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if after := eventFingerprint(t, o.db); after != before {
		t.Fatal("bounded migration changed historical logical rows")
	}
	var integrity string
	if err = o.db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatal("bounded migration corrupted ledger")
	}
}
