package capture

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	_ "github.com/mattn/go-sqlite3"
)

type Outbox struct {
	db                 *sql.DB
	path               string
	reserve            uint64
	budget             int64
	destinationBudgets map[string]int64
}

func OpenOutbox(path string, maxBytes int64, reserve uint64) (*Outbox, error) {
	if !filepath.IsAbs(path) || maxBytes < 4<<20 {
		return nil, errors.New("invalid outbox configuration")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	if s, err := os.Stat(filepath.Dir(path)); err != nil || s.Mode().Perm()&0077 != 0 {
		return nil, errors.New("outbox directory must be private")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	f.Close()
	db, err := sql.Open("sqlite3", "file:"+path+"?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("PRAGMA auto_vacuum=INCREMENTAL"); err != nil {
		db.Close()
		return nil, err
	}
	o := &Outbox{db: db, path: path, reserve: reserve, budget: maxBytes}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events(identity TEXT PRIMARY KEY,digest TEXT NOT NULL,destination TEXT NOT NULL,instance TEXT NOT NULL,boot TEXT NOT NULL,request_id TEXT NOT NULL,sequence INTEGER NOT NULL,kind TEXT NOT NULL,payload BLOB NOT NULL,received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,state TEXT NOT NULL DEFAULT 'pending'); CREATE INDEX IF NOT EXISTS events_call ON events(destination,instance,boot,request_id,sequence);`)
	if err == nil {
		_, err = db.Exec("PRAGMA max_page_count = " + itoa(maxBytes/4096))
	}
	if err == nil {
		err = o.initDelivery()
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return o, nil
}
func itoa(n int64) string      { b, _ := json.Marshal(n); return string(b) }
func (o *Outbox) Close() error { return o.db.Close() }
func (o *Outbox) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" && r.URL.Path == "/status" {
		var count, bytes int64
		err := o.db.QueryRow("SELECT count(*),coalesce(sum(length(payload)),0) FROM events").Scan(&count, &bytes)
		if err != nil {
			http.Error(w, "outbox unavailable", 503)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"delivery": o.deliveryStatus(), "durableEvents": count, "payloadBytes": bytes, "remoteDelivery": "durable_batches", "retention": "indefinite", "precommitCoverage": "unknown"})
		return
	}
	if r.Method == "POST" && r.URL.Path == "/health" {
		o.admitHealth(w, r)
		return
	}
	if r.Method == "POST" && r.URL.Path == "/resume" {
		var request struct {
			Destination string `json:"destinationId"`
		}
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 1024))
		if json.Unmarshal(raw, &request) != nil || !identifier.MatchString(request.Destination) {
			http.Error(w, "invalid destination", 400)
			return
		}
		_, err := o.db.Exec("UPDATE batches SET state='pending',next_attempt=0 WHERE destination=? AND state='quarantined'", request.Destination)
		if err == nil {
			_, err = o.db.Exec("UPDATE delivery_status SET state='pending',reason='operator_resume' WHERE destination=?", request.Destination)
		}
		if err != nil {
			http.Error(w, "resume unavailable", 503)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"resumed": true})
		return
	}
	if r.Method != "POST" || r.URL.Path != "/events" {
		http.NotFound(w, r)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, MaxFrame+1))
	if err != nil || len(raw) > MaxFrame {
		http.Error(w, "envelope limit", 413)
		return
	}
	var event Observation
	if json.Unmarshal(raw, &event) != nil || event.Validate() != nil {
		http.Error(w, "invalid event", 400)
		return
	}
	identity, digest := event.Identity(), Digest(raw)
	tx, err := o.db.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, "outbox unavailable", 503)
		return
	}
	defer tx.Rollback()
	var existing string
	err = tx.QueryRow("SELECT digest FROM events WHERE identity=?", identity).Scan(&existing)
	if err == nil {
		if existing != digest {
			http.Error(w, "identity conflict", 409)
			return
		}
	} else if err == sql.ErrNoRows {
		if limit, ok := o.destinationBudgets[event.Destination]; ok && !(len(event.Body) == 0 && strings.HasPrefix(event.Gap, "capture_queue_")) {
			var pending int64
			if tx.QueryRow("SELECT coalesce(sum(length(payload)),0) FROM events WHERE destination=? AND state='pending'", event.Destination).Scan(&pending) != nil || pending+int64(len(raw)) > limit {
				http.Error(w, "destination outbox budget", 507)
				return
			}
		}
		var pages, freePages int64
		if !(len(event.Body) == 0 && strings.HasPrefix(event.Gap, "capture_queue_")) && (tx.QueryRow("PRAGMA page_count").Scan(&pages) != nil || tx.QueryRow("PRAGMA freelist_count").Scan(&freePages) != nil || (pages-freePages)*4096+int64(len(raw)*2) > o.budget-min(o.budget/8, 1<<20)) {
			http.Error(w, "reserved control capacity", 507)
			return
		}
		var stat syscall.Statfs_t
		if syscall.Statfs(filepath.Dir(o.path), &stat) != nil || uint64(stat.Bavail)*uint64(stat.Bsize) < o.reserve+uint64(len(raw)*4) {
			http.Error(w, "disk reserve", 507)
			return
		}
		_, err = tx.Exec("INSERT INTO events(identity,digest,destination,instance,boot,request_id,sequence,kind,payload) VALUES (?,?,?,?,?,?,?,?,?)", identity, digest, event.Destination, event.Instance, event.Boot, event.RequestID, event.Sequence, event.Kind, raw)
		if err != nil {
			http.Error(w, "outbox capacity or write failure", 507)
			return
		}
	} else {
		http.Error(w, "outbox unavailable", 503)
		return
	}
	if tx.Commit() != nil {
		http.Error(w, "outbox commit failure", 503)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"identity": identity, "digest": digest})
}
