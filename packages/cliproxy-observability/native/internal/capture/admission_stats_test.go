package capture

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestLocalAdmissionCountsLatencyAndFailureClasses(t *testing.T) {
	var a admissionCounters
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			a.record(time.Now().Add(-time.Millisecond), 200, true)
			a.record(time.Now(), 507, false)
			a.record(time.Now(), 409, false)
			recordPeak(&a.queuePeak, 17)
			recordPeak(&a.controlPeak, 4)
		}()
	}
	workers.Wait()
	s := a.snapshot()
	if s.Attempts != 60 || s.Accepted != 20 || s.Rejected507 != 20 || s.OtherFailures != 20 || s.LatencyMaxNS < 1000000 || s.LatencyTotalNS < s.LatencyMaxNS || s.QueuePeakItems != 17 || s.ControlPeakItems != 4 {
		t.Fatal(s)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	h := Health{LocalAdmission: s, SchemaVersion: 1, Operation: "health_record", Destination: "dev", Instance: "instance", Boot: "boot", StartedAt: now, ObservedAt: now, PrecommitCoverage: "unknown_before_local_commit"}
	raw, _ := json.Marshal(h)
	o := deliveryOutbox(t)
	res := httptest.NewRecorder()
	o.ServeHTTP(res, httptest.NewRequest("POST", "/health", bytes.NewReader(raw)))
	if res.Code != 200 {
		t.Fatal(res.Code)
	}
	var saved []byte
	if err := o.db.QueryRow("SELECT payload FROM health WHERE boot='boot'").Scan(&saved); err != nil {
		t.Fatal(err)
	}
	var persisted Health
	json.Unmarshal(saved, &persisted)
	if persisted.LocalAdmission == nil || *persisted.LocalAdmission != *s {
		t.Fatal("local health lost admission stats")
	}
}
