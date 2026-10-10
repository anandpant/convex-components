package capture

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Outbox struct {
	capacityMu         sync.Mutex
	capacityAt         time.Time
	capacityBytes      int64
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
	// SQLite ignores auto_vacuum on an existing NONE database until VACUUM.
	// Perform this once, before admission starts; preserve all unacknowledged rows.
	var vacuumMode int
	if err = db.QueryRow("PRAGMA auto_vacuum").Scan(&vacuumMode); err != nil {
		db.Close()
		return nil, err
	}
	if vacuumMode != 2 {
		var pages, pageSize uint64
		var fs syscall.Statfs_t
		if db.QueryRow("PRAGMA page_count").Scan(&pages) != nil || db.QueryRow("PRAGMA page_size").Scan(&pageSize) != nil || syscall.Statfs(filepath.Dir(path), &fs) != nil || uint64(fs.Bavail)*uint64(fs.Bsize) < reserve+pages*pageSize*2 {
			db.Close()
			return nil, errors.New("insufficient disk reserve for outbox vacuum migration")
		}
		if _, err = db.Exec("VACUUM"); err != nil {
			db.Close()
			return nil, err
		}
		if err = db.QueryRow("PRAGMA auto_vacuum").Scan(&vacuumMode); err != nil || vacuumMode != 2 {
			db.Close()
			return nil, errors.New("outbox vacuum migration did not enable incremental reclamation")
		}
	}
	o := &Outbox{db: db, path: path, reserve: reserve, budget: maxBytes}
	var eventKind string
	_ = db.QueryRow("SELECT type FROM sqlite_master WHERE name='events'").Scan(&eventKind)
	if eventKind != "view" {
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events(identity TEXT PRIMARY KEY,digest TEXT NOT NULL,destination TEXT NOT NULL,instance TEXT NOT NULL,boot TEXT NOT NULL,request_id TEXT NOT NULL,sequence INTEGER NOT NULL,kind TEXT NOT NULL,payload BLOB NOT NULL,received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,state TEXT NOT NULL DEFAULT 'pending'); CREATE INDEX IF NOT EXISTS events_call ON events(destination,instance,boot,request_id,sequence);`)
	}
	if err == nil {
		// SQLite's hard ceiling applies to delivery bookkeeping too. Keep bounded
		// drain space outside the event-admission budget so a full legacy database
		// can persist its first batch and exact ACK without admitting more events.
		_, err = db.Exec("PRAGMA max_page_count = " + itoa((maxBytes+min(maxBytes/8, 8<<20))/4096))
	}
	if err == nil {
		err = o.initDelivery()
	}
	if err == nil {
		err = o.compactAtAdmissionCeiling()
	}
	if err == nil {
		err = o.initReceipts()
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return o, nil
}

func (o *Outbox) compactAtAdmissionCeiling() error {
	var pages, pageSize int64
	if err := o.db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		return err
	}
	if err := o.db.QueryRow("PRAGMA page_size").Scan(&pageSize); err != nil {
		return err
	}
	allocated := pages * pageSize
	if allocated < o.budget-min(o.budget/8, 1<<20) {
		return nil
	}
	// Clearing ACKed payloads can leave sparse live pages, which incremental
	// vacuum cannot repack. Compact only at startup, before admission/delivery,
	// with the same conservative temporary-space check as the legacy migration.
	var fs syscall.Statfs_t
	if syscall.Statfs(filepath.Dir(o.path), &fs) != nil || uint64(fs.Bavail)*uint64(fs.Bsize) < o.reserve+uint64(allocated)*2 {
		return nil // Insufficient compaction space must not disable pending delivery.
	}
	_, err := o.db.Exec("VACUUM")
	return err
}
func itoa(n int64) string      { b, _ := json.Marshal(n); return string(b) }
func (o *Outbox) Close() error { return o.db.Close() }
func (o *Outbox) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" && r.URL.Path == "/status" {
		var count, bytes int64
		err := o.db.QueryRow("SELECT pending_rows+receipt_rows,payload_bytes FROM outbox_counts WHERE id=1").Scan(&count, &bytes)
		if err != nil {
			http.Error(w, "outbox unavailable", 503)
			return
		}
		capacity, err := o.capacity()
		if err != nil {
			http.Error(w, "capacity unavailable", 503)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"capacity": capacity, "delivery": o.deliveryStatus(), "durableEvents": count, "payloadBytes": bytes, "remoteDelivery": "durable_batches", "retention": "indefinite", "precommitCoverage": "unknown"})
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
	if r.Method != "POST" || (r.URL.Path != "/events" && r.URL.Path != "/events/batch") {
		http.NotFound(w, r)
		return
	}
	o.admitEvents(w, r, r.URL.Path == "/events/batch")
}
