package capture

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func readCapacity(t *testing.T, o *Outbox) Capacity {
	t.Helper()
	r := httptest.NewRecorder()
	o.ServeHTTP(r, httptest.NewRequest("GET", "/status", nil))
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	var body struct {
		Capacity Capacity `json:"capacity"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Capacity
}
func TestCapacityReportsAdmissionAndACKWithoutInventingSteadyForecast(t *testing.T) {
	o := deliveryOutbox(t)
	initial := readCapacity(t, o)
	if initial.PendingBytes != 0 || initial.AcknowledgedEvents != 0 || initial.EstimatedSecondsToCeiling != nil {
		t.Fatal("empty database capacity is incorrect")
	}
	time.Sleep(1100 * time.Millisecond)
	raw := drainEvent(1, 200<<10)
	if res := postDrainEvent(o, raw); res.Code != 200 {
		t.Fatal(res.Body.String())
	}
	admitted := readCapacity(t, o)
	if admitted.PendingBytes != int64(len(raw)) || admitted.AllocatedBytes <= initial.AllocatedBytes || admitted.RemainingBytes >= initial.RemainingBytes || admitted.EstimatedSecondsToCeiling == nil {
		t.Fatal("admission growth/capacity not reported")
	}
	batch, err := o.nextBatch(Destination{ID: "prod", Instance: "instance"})
	if err != nil {
		t.Fatal(err)
	}
	if err = o.ackBatch(*batch); err != nil {
		t.Fatal(err)
	}
	delivered := readCapacity(t, o)
	if delivered.PendingBytes != 0 || delivered.AcknowledgedEvents != 1 {
		t.Fatal("ACK capacity counters lost")
	}
	path := o.path
	o.Close()
	o, err = OpenOutbox(path, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	reopened := readCapacity(t, o)
	if reopened.PendingBytes != 0 || reopened.AcknowledgedEvents != 1 || reopened.EstimatedSecondsToCeiling != nil {
		t.Fatal("restart changed counts or fabricated a growth history")
	}
}
