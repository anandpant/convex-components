package capture

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReadySelectionOrderExclusionsAndQueryPlan(t *testing.T) {
	o := deliveryOutbox(t)
	tx, err := o.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i, spec := range []struct {
		request, destination, instance, date, state string
		sequence                                    uint64
		assigned                                    bool
	}{
		{"b", "dev", "test", "2026-10-01 00:00:00", "pending", 1, false},
		{"a", "dev", "test", "2026-10-01 00:00:00", "pending", 2, false},
		{"a", "dev", "test", "2026-10-01 00:00:00", "pending", 1, false},
		{"a", "dev", "test", "2026-09-01 00:00:00", "pending", 3, true},
		{"delivered", "dev", "test", "2026-09-01 00:00:00", "delivered", 1, false},
		{"other-destination", "prod", "test", "2026-09-01 00:00:00", "pending", 1, false},
		{"other-instance", "dev", "other", "2026-09-01 00:00:00", "pending", 1, false},
	} {
		var e Observation
		json.Unmarshal(batchEvent(spec.sequence), &e)
		e.RequestID, e.Destination, e.Instance = spec.request, spec.destination, spec.instance
		raw, _ := json.Marshal(e)
		var batch any
		if spec.assigned {
			batch = "already-assigned"
		}
		if _, err = tx.Exec("INSERT INTO pending_events(identity,digest,destination,instance,boot,request_id,sequence,kind,payload,received_at,state,batch_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", e.Identity(), Digest(raw), e.Destination, e.Instance, e.Boot, e.RequestID, e.Sequence, e.Kind, raw, spec.date, spec.state, batch); err != nil {
			t.Fatal(i, err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err := o.db.Query("EXPLAIN QUERY PLAN "+firstReadyEventQuery, "dev", "test")
	if err != nil {
		t.Fatal(err)
	}
	usesReady := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "TEMP B-TREE") {
			t.Fatal("ready lookup sorts payloads", detail)
		}
		usesReady = usesReady || strings.Contains(detail, "events_ready")
	}
	err = rows.Err()
	rows.Close()
	if err != nil || !usesReady {
		t.Fatal("ready index/predicate drift", err)
	}
	b, err := o.nextBatch(Destination{ID: "dev", Instance: "test"})
	if err != nil || b == nil || b.Request != "a" || b.First != 1 || b.Through != 2 {
		t.Fatal("ready ordering or exclusions changed", b, err)
	}
}
