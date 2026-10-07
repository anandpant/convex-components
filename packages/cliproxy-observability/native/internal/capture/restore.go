package capture

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// RestoreLegacyOutbox reverses the representation, including observations made
// after migration. The exporter must be stopped. Restoring an old snapshot would
// discard new data; this transaction copies the current complete logical ledger.
func RestoreLegacyOutbox(path string, budget int64, reserve uint64) error {
	if !filepath.IsAbs(path) || budget < 4<<20 {
		return errors.New("invalid restore configuration")
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return err
	}
	if parent.Mode().Perm()&0077 != 0 {
		return errors.New("outbox directory must be private")
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0077 != 0 {
		return errors.New("outbox must be private")
	}
	db, err := sql.Open("sqlite3", "file:"+path+"?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000&_txlock=exclusive")
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var kind string
	if err = db.QueryRow("SELECT type FROM sqlite_master WHERE name='events'").Scan(&kind); err != nil {
		return err
	}
	if kind == "table" {
		return nil
	}
	if kind != "view" {
		return errors.New("unsupported receipt schema")
	}
	var check string
	if err = db.QueryRow("PRAGMA quick_check").Scan(&check); err != nil {
		return err
	}
	if check != "ok" {
		return errors.New("outbox integrity check failed")
	}
	var allocated int64
	if err = db.QueryRow("SELECT page_count*page_size FROM pragma_page_count(),pragma_page_size()").Scan(&allocated); err != nil {
		return err
	}
	var fs syscall.Statfs_t
	if syscall.Statfs(filepath.Dir(path), &fs) != nil || uint64(fs.Bavail)*uint64(fs.Bsize) < reserve+uint64(allocated)*2 {
		return errors.New("insufficient disk reserve for legacy restore")
	}
	if _, err = db.Exec("PRAGMA max_page_count=" + itoa((budget+min(budget/8, 8<<20))/4096)); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`
 CREATE TABLE legacy_events(identity TEXT PRIMARY KEY,digest TEXT NOT NULL,destination TEXT NOT NULL,instance TEXT NOT NULL,boot TEXT NOT NULL,request_id TEXT NOT NULL,sequence INTEGER NOT NULL,kind TEXT NOT NULL,payload BLOB NOT NULL,received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,state TEXT NOT NULL DEFAULT 'pending',batch_id TEXT);
 INSERT INTO legacy_events SELECT identity,digest,destination,instance,boot,request_id,sequence,kind,payload,received_at,state,batch_id FROM events;
 DROP VIEW events;
 DROP TABLE pending_events;
 DROP TABLE acknowledged_events;
 DROP TABLE receipt_scopes;
 DROP TABLE receipt_batches;
 DROP TABLE outbox_counts;
 ALTER TABLE legacy_events RENAME TO events;
 CREATE INDEX events_call ON events(destination,instance,boot,request_id,sequence);
 CREATE INDEX events_pending ON events(destination,state,received_at);
 `); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if _, err = db.Exec("VACUUM"); err != nil {
		return err
	}
	if err = db.QueryRow("PRAGMA integrity_check").Scan(&check); err != nil {
		return err
	}
	if check != "ok" {
		return errors.New("restored outbox integrity check failed")
	}
	return nil
}
