package capture

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func assertDestinationCounts(t *testing.T, o *Outbox) {
	t.Helper()
	expected := map[string]int64{}
	rows, err := o.db.Query("SELECT destination,payload FROM pending_events WHERE state='pending'")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var d string
		var raw []byte
		if err = rows.Scan(&d, &raw); err != nil {
			t.Fatal(err)
		}
		expected[d] += int64(len(raw))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	rows, err = o.db.Query("SELECT destination,payload_bytes FROM destination_counts")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var d string
		var n int64
		if err = rows.Scan(&d, &n); err != nil {
			t.Fatal(err)
		}
		if n != expected[d] {
			t.Fatalf("destination %s: counter%d expected%d", d, n, expected[d])
		}
		delete(expected, d)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(expected) != 0 {
		t.Fatal("missing destination counter", expected)
	}
}

func TestDestinationCountsCoverEveryPendingWrite(t *testing.T) {
	o := deliveryOutbox(t)
	assertDestinationCounts(t, o)
	for i := 1; i <= 3; i++ {
		admitTestEvent(t, o, "dev", uint64(i))
	}
	admitTestEvent(t, o, "prod", 1)
	assertDestinationCounts(t, o)
	statements := []string{
		"UPDATE pending_events SET payload=CAST(payload||x'00' AS BLOB) WHERE destination='dev' AND sequence=1",
		"UPDATE pending_events SET state='quarantined' WHERE destination='dev' AND sequence=2",
		"UPDATE pending_events SET state='pending',destination='other' WHERE sequence=2",
		"UPDATE pending_events SET destination='prod' WHERE destination='dev'",
	}
	for _, q := range statements {
		if _, err := o.db.Exec(q); err != nil {
			t.Fatal(err)
		}
		assertDestinationCounts(t, o)
	}
	tx, _ := o.db.Begin()
	if _, err := tx.Exec("DELETE FROM pending_events"); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	assertDestinationCounts(t, o)
	if _, err := o.db.Exec("DELETE FROM pending_events"); err != nil {
		t.Fatal(err)
	}
	assertDestinationCounts(t, o)
	var n int
	o.db.QueryRow("SELECT count(*) FROM destination_counts WHERE payload_bytes!=0").Scan(&n)
	if n != 0 {
		t.Fatal("empty destinations retained bytes")
	}
}

func TestDestinationBudgetDuplicateAckAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "events.db")
	o, err := OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw := drainEvent(1, 1024)
	var e Observation
	json.Unmarshal(raw, &e)
	o.destinationBudgets = map[string]int64{e.Destination: int64(len(raw))}
	if r := postDrainEvent(o, raw); r.Code != 200 {
		t.Fatal(r.Code)
	}
	if r := postDrainEvent(o, raw); r.Code != 200 {
		t.Fatal("exact duplicate charged twice", r.Code)
	}
	modified := bytes.Replace(raw, []byte("stream_chunk"), []byte("other_kind"), 1)
	if r := postDrainEvent(o, modified); r.Code != 507 {
		t.Fatal("destination budget not enforced", r.Code)
	}
	e.Body = []byte("changed")
	e.ContentBytes = len(e.Body)
	e.ContentSHA256 = Digest(e.Body)
	changed, _ := json.Marshal(e)
	if r := postDrainEvent(o, changed); r.Code != 409 {
		t.Fatal("identity conflict changed", r.Code)
	}
	o.destinationBudgets[e.Destination] = 1 << 20
	if r := postDrainEvent(o, drainEvent(2, 1024)); r.Code != 200 {
		t.Fatal(r.Code)
	}
	b, err := o.nextBatch(Destination{ID: e.Destination, Instance: e.Instance})
	if err != nil || b == nil {
		t.Fatal(err)
	}
	if err = o.resegment(*b); err != nil {
		t.Fatal(err)
	}
	assertDestinationCounts(t, o)
	b, err = o.nextBatch(Destination{ID: e.Destination, Instance: e.Instance})
	if err != nil || b == nil {
		t.Fatal(err)
	}
	if err = o.ackBatch(*b); err != nil {
		t.Fatal(err)
	}
	assertDestinationCounts(t, o)
	if r := postDrainEvent(o, raw); r.Code != 200 {
		t.Fatal("ACK receipt lost exact replay", r.Code)
	}
	before := eventFingerprint(t, o.db)
	o.Close()
	o, err = OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if eventFingerprint(t, o.db) != before {
		t.Fatal("reopen changed ledger")
	}
	assertDestinationCounts(t, o)
}

func TestDestinationCountsStartupRejectsMismatch(t *testing.T) {
	for _, damage := range []string{"UPDATE destination_counts SET payload_bytes=payload_bytes+1", "DELETE FROM destination_counts", "DROP TRIGGER destination_pending_added"} {
		t.Run(damage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private", "events.db")
			o, err := OpenOutbox(path, 64<<20, 0)
			if err != nil {
				t.Fatal(err)
			}
			admitTestEvent(t, o, "dev", 1)
			before := eventFingerprint(t, o.db)
			if _, err = o.db.Exec(damage); err != nil {
				t.Fatal(err)
			}
			o.Close()
			reopened, err := OpenOutbox(path, 64<<20, 0)
			if reopened != nil {
				reopened.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "counter mismatch") {
				t.Fatal("corrupt counts silently accepted", err)
			}
			db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if eventFingerprint(t, db) != before {
				t.Fatal("failed startup changed logical rows")
			}
		})
	}
}

func TestCurrentReceiptsInitializeDestinationCountsWithoutChangingRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "events.db")
	o, err := OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	admitTestEvent(t, o, "dev", 1)
	admitTestEvent(t, o, "prod", 1)
	before := eventFingerprint(t, o.db)
	if _, err = o.db.Exec("DROP TRIGGER destination_pending_added; DROP TRIGGER destination_pending_removed; DROP TRIGGER destination_pending_changed; DROP TABLE destination_counts"); err != nil {
		t.Fatal(err)
	}
	o.Close()
	o, err = OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	assertDestinationCounts(t, o)
	if eventFingerprint(t, o.db) != before {
		t.Fatal("current receipt migration changed a logical row")
	}
}
