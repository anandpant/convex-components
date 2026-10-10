package capture

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"syscall"
)

const maxAdmissionEvents = 128

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
	lookup, err := tx.Prepare("SELECT digest FROM pending_events WHERE identity=? UNION ALL SELECT lower(hex(digest)) FROM acknowledged_events WHERE identity=?")
	if err != nil {
		http.Error(w, "outbox unavailable", 503)
		return
	}
	defer lookup.Close()
	newRecords := make([]int, 0, len(records))
	seen := make(map[string]string, len(records))
	var bytes, chargedBytes int64
	controlsOnly := true
	for i, ack := range acks {
		existing, duplicate := seen[ack.Identity]
		if !duplicate {
			err = lookup.QueryRow(ack.Identity, ack.Identity).Scan(&existing)
			duplicate = err == nil
			if err != nil && err != sql.ErrNoRows {
				http.Error(w, "outbox unavailable", 503)
				return
			}
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
			chargedBytes += int64(len(records[i]))
			controlsOnly = false
		}
	}
	if len(newRecords) > 0 {
		if limit, ok := o.destinationBudgets[events[0].Destination]; ok && chargedBytes > 0 {
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
		insert, err := tx.Prepare("INSERT INTO pending_events(identity,digest,destination,instance,boot,request_id,sequence,kind,payload) VALUES (?,?,?,?,?,?,?,?,?)")
		if err != nil {
			http.Error(w, "outbox unavailable", 503)
			return
		}
		defer insert.Close()
		for _, i := range newRecords {
			e := events[i]
			if _, err = insert.Exec(acks[i].Identity, acks[i].Digest, e.Destination, e.Instance, e.Boot, e.RequestID, e.Sequence, e.Kind, records[i]); err != nil {
				http.Error(w, "outbox capacity or write failure", 507)
				return
			}
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
