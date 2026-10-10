package capture

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func batchEvent(sequence uint64) json.RawMessage {
	e := Observation{SchemaVersion: 1, Destination: "dev", Instance: "test", Boot: "batch", RequestID: "call", Sequence: sequence, Kind: "stream_chunk", Route: "POST /v1/responses", Body: []byte("data: bounded\n\n")}
	e.ContentBytes, e.ContentSHA256 = len(e.Body), Digest(e.Body)
	raw, _ := json.Marshal(e)
	return raw
}

func sendEventBatch(t *testing.T, o *Outbox, records ...json.RawMessage) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRecorder()
	o.ServeHTTP(r, httptest.NewRequest("POST", "/events/batch", bytes.NewReader(raw)))
	return r
}

func TestAdmissionBatchAtomicReplayConflictAndLimits(t *testing.T) {
	for _, failure := range []string{"conflict", "write", "budget", "invalid", "count"} {
		t.Run(failure, func(t *testing.T) {
			o := deliveryOutbox(t)
			records := []json.RawMessage{batchEvent(1), batchEvent(2)}
			want := 200
			switch failure {
			case "conflict":
				sendEventBatch(t, o, records[1])
				var e Observation
				json.Unmarshal(records[1], &e)
				e.Body = []byte("different")
				e.ContentBytes, e.ContentSHA256 = len(e.Body), Digest(e.Body)
				records[1], _ = json.Marshal(e)
				want = 409
			case "write":
				_, err := o.db.Exec("CREATE TRIGGER fail_second BEFORE INSERT ON pending_events WHEN NEW.sequence=2 BEGIN SELECT RAISE(ABORT,'simulated write failure'); END")
				if err != nil {
					t.Fatal(err)
				}
				want = 507
			case "budget":
				o.destinationBudgets = map[string]int64{"dev": int64(len(records[0])) + 1}
				want = 507
			case "invalid":
				records[1] = json.RawMessage(`{"schemaVersion":99}`)
				want = 400
			case "count":
				for len(records) <= maxAdmissionEvents {
					records = append(records, batchEvent(uint64(len(records)+1)))
				}
				want = 400
			}
			result := sendEventBatch(t, o, records...)
			if result.Code != want {
				t.Fatal(result.Code, result.Body.String())
			}
			var count, pendingBytes int
			o.db.QueryRow("SELECT pending_rows,payload_bytes FROM outbox_counts WHERE id=1").Scan(&count, &pendingBytes)
			if (failure == "conflict" && count != 1) || (failure != "conflict" && (count != 0 || pendingBytes != 0)) {
				t.Fatal("partial group survived failure", count, pendingBytes)
			}
		})
	}
	o := deliveryOutbox(t)
	records := []json.RawMessage{batchEvent(1), batchEvent(2), batchEvent(1)}
	for retry := 0; retry < 2; retry++ {
		r := sendEventBatch(t, o, records...)
		var acks []eventACK
		if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &acks) != nil || len(acks) != len(records) {
			t.Fatal("no exact group ACK", r.Code)
		}
		for i, raw := range records {
			var e Observation
			json.Unmarshal(raw, &e)
			if acks[i] != (eventACK{e.Identity(), Digest(raw)}) {
				t.Fatal("record identity changed")
			}
		}
	}
	var count int
	o.db.QueryRow("SELECT pending_rows FROM outbox_counts WHERE id=1").Scan(&count)
	if count != 2 {
		t.Fatal("retry created duplicate records", count)
	}
}

func TestAdmissionBatchReservedControlAndFrame(t *testing.T) {
	o := deliveryOutbox(t)
	o.destinationBudgets = map[string]int64{"dev": 1}
	var gap Observation
	json.Unmarshal(batchEvent(1), &gap)
	gap.Body, gap.ContentBytes, gap.ContentSHA256, gap.Gap = nil, 0, Digest(nil), "capture_queue_item_limit"
	raw, _ := json.Marshal(gap)
	if r := sendEventBatch(t, o, raw); r.Code != 200 {
		t.Fatal("reserved control admission disabled", r.Code)
	}
	r := httptest.NewRecorder()
	o.ServeHTTP(r, httptest.NewRequest("POST", "/events/batch", bytes.NewReader(bytes.Repeat([]byte("x"), MaxFrame+1))))
	if r.Code != 413 {
		t.Fatal("frame bound changed", r.Code)
	}
}
