package capture

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"syscall"
)

const maxAdmissionEvents = 512

type eventACK struct {
	Identity string `json:"identity"`
	Digest   string `json:"digest"`
}

// Both endpoints ACK exact serialized records only after one FULL-sync commit.
// A failed group rolls back every new record; retries retain their identities.
func (o *Outbox) admitEvents(w http.ResponseWriter, r *http.Request, batch bool) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, MaxFrame+1))
	if err != nil || len(raw) > MaxFrame {
		http.Error(w, "envelope limit", 413)
		return
	}
	records := []json.RawMessage{raw}
	if batch && (json.Unmarshal(raw, &records) != nil || len(records) == 0 || len(records) > maxAdmissionEvents) {
		http.Error(w, "invalid event batch", 400)
		return
	}
	acks := make([]eventACK, len(records))
	events := make([]Observation, len(records))
	for i, raw := range records {
		if json.Unmarshal(raw, &events[i]) != nil || events[i].Validate() != nil || (i > 0 && events[i].Destination != events[0].Destination) {
			http.Error(w, "invalid event", 400)
			return
		}
		acks[i] = eventACK{events[i].Identity(), Digest(raw)}
	}
	tx, err := o.db.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, "outbox unavailable", 503)
		return
	}
	defer tx.Rollback()
	marks := strings.TrimSuffix(strings.Repeat("?,", len(acks)), ",")
	keys := make([]any, 0, len(acks)*2)
	for repeat := 0; repeat < 2; repeat++ {
		for _, ack := range acks {
			keys = append(keys, ack.Identity)
		}
	}
	rows, err := tx.Query("SELECT identity,digest FROM pending_events WHERE identity IN ("+marks+") UNION ALL SELECT identity,lower(hex(digest)) FROM acknowledged_events WHERE identity IN ("+marks+")", keys...)
	if err != nil {
		http.Error(w, "outbox unavailable", 503)
		return
	}
	existingRecords := make(map[string]string, len(acks))
	for rows.Next() {
		var id, digest string
		if err = rows.Scan(&id, &digest); err != nil {
			break
		}
		if prior, exists := existingRecords[id]; exists && prior != digest {
			err = errors.New("conflicting durable identity")
			break
		}
		existingRecords[id] = digest
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		http.Error(w, "outbox unavailable", 503)
		return
	}
	newRecords := make([]int, 0, len(records))
	seen := make(map[string]string, len(records))
	var bytes int64
	controlsOnly := true
	for i, ack := range acks {
		existing, duplicate := seen[ack.Identity]
		if !duplicate {
			existing, duplicate = existingRecords[ack.Identity]
		}
		if duplicate {
			if existing != ack.Digest {
				http.Error(w, "identity conflict", 409)
				return
			}
			continue
		}
		seen[ack.Identity] = ack.Digest
		newRecords = append(newRecords, i)
		bytes += int64(len(records[i]))
		if !(len(events[i].Body) == 0 && strings.HasPrefix(events[i].Gap, "capture_queue_")) {
			controlsOnly = false
		}
	}
	if len(newRecords) > 0 {
		if limit, ok := o.destinationBudgets[events[0].Destination]; ok && !controlsOnly {
			var pending int64
			if tx.QueryRow("SELECT coalesce((SELECT payload_bytes FROM destination_counts WHERE destination=?),0)", events[0].Destination).Scan(&pending) != nil || pending+bytes > limit {
				http.Error(w, "destination outbox budget", 507)
				return
			}
		}
		admissionBudget := o.budget - min(o.budget/8, 1<<20)
		if controlsOnly {
			admissionBudget = o.budget
		}
		var pages, freePages int64
		if tx.QueryRow("PRAGMA page_count").Scan(&pages) != nil || tx.QueryRow("PRAGMA freelist_count").Scan(&freePages) != nil || (pages-freePages)*4096+bytes*2 > admissionBudget {
			http.Error(w, "reserved control capacity", 507)
			return
		}
		var stat syscall.Statfs_t
		if syscall.Statfs(filepath.Dir(o.path), &stat) != nil || uint64(stat.Bavail)*uint64(stat.Bsize) < o.reserve+uint64(bytes*4) {
			http.Error(w, "disk reserve", 507)
			return
		}
		values := strings.TrimSuffix(strings.Repeat("(?,?,?,?,?,?,?,?,?),", len(newRecords)), ",")
		args := make([]any, 0, len(newRecords)*9)
		for _, i := range newRecords {
			e := events[i]
			args = append(args, acks[i].Identity, acks[i].Digest, e.Destination, e.Instance, e.Boot, e.RequestID, e.Sequence, e.Kind, records[i])
		}
		if _, err = tx.Exec("INSERT INTO pending_events(identity,digest,destination,instance,boot,request_id,sequence,kind,payload) VALUES "+values, args...); err != nil {
			http.Error(w, "outbox capacity or write failure", 507)
			return
		}
	}
	if tx.Commit() != nil {
		http.Error(w, "outbox commit failure", 503)
		return
	}
	if batch {
		json.NewEncoder(w).Encode(acks)
	} else {
		json.NewEncoder(w).Encode(acks[0])
	}
}
