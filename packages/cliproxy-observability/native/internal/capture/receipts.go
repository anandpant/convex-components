package capture

import (
	"database/sql"
	"errors"
	"path/filepath"
	"syscall"
)

// The events view preserves the historical logical row contract. Only pending
// payloads live in the mutable table; exact ACK receipts never share their pages.
func (o *Outbox) initReceipts() error {
	var kind string
	if err := o.db.QueryRow("SELECT type FROM sqlite_master WHERE name='events'").Scan(&kind); err != nil {
		return err
	}
	if kind == "table" {
		var allocated int64
		if err := o.db.QueryRow("SELECT page_count*page_size FROM pragma_page_count(),pragma_page_size()").Scan(&allocated); err != nil {
			return err
		}
		var fs syscall.Statfs_t
		if syscall.Statfs(filepath.Dir(o.path), &fs) != nil || uint64(fs.Bavail)*uint64(fs.Bsize) < o.reserve+uint64(allocated)*2 {
			return errors.New("insufficient disk reserve for receipt migration")
		}
		// Repack legacy payload updates before allocating the new representation.
		// The admission/hard budgets remain unchanged throughout the migration.
		if _, err := o.db.Exec("VACUUM"); err != nil {
			return err
		}
	}
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`
 CREATE TABLE IF NOT EXISTS receipt_scopes(id INTEGER PRIMARY KEY,destination TEXT NOT NULL,instance TEXT NOT NULL,boot TEXT NOT NULL,request_id TEXT NOT NULL,UNIQUE(destination,instance,boot,request_id));
 CREATE TABLE IF NOT EXISTS receipt_batches(id INTEGER PRIMARY KEY,batch_id TEXT NOT NULL UNIQUE);
 CREATE TABLE IF NOT EXISTS acknowledged_events(identity TEXT PRIMARY KEY,digest BLOB NOT NULL,scope INTEGER NOT NULL REFERENCES receipt_scopes(id),sequence INTEGER NOT NULL,kind TEXT NOT NULL,received_at TEXT NOT NULL,batch INTEGER REFERENCES receipt_batches(id)) WITHOUT ROWID;
 `); err != nil {
		return err
	}
	if kind == "table" {
		if _, err = tx.Exec("ALTER TABLE events RENAME TO pending_events; DROP INDEX events_call; DROP INDEX events_pending"); err != nil {
			return err
		}
	}
	if err = moveReceipts(tx, "state='delivered' AND length(payload)=0"); err != nil {
		return err
	}
	if _, err = tx.Exec(`
 CREATE INDEX IF NOT EXISTS events_call ON pending_events(destination,instance,boot,request_id,sequence);
 CREATE INDEX IF NOT EXISTS events_pending ON pending_events(destination,state,received_at);
 CREATE INDEX IF NOT EXISTS events_batch ON pending_events(batch_id);
 CREATE INDEX IF NOT EXISTS events_ready ON pending_events(destination,instance,received_at,request_id,sequence) WHERE state='pending' AND batch_id IS NULL;
 CREATE VIEW IF NOT EXISTS events AS
 SELECT identity,digest,destination,instance,boot,request_id,sequence,kind,payload,received_at,state,batch_id FROM pending_events
 UNION ALL
 SELECT r.identity,lower(hex(r.digest)),s.destination,s.instance,s.boot,s.request_id,r.sequence,r.kind,x'',r.received_at,'delivered',b.batch_id FROM acknowledged_events r JOIN receipt_scopes s ON s.id=r.scope LEFT JOIN receipt_batches b ON b.id=r.batch;
 CREATE TABLE IF NOT EXISTS outbox_counts(id INTEGER PRIMARY KEY CHECK(id=1),pending_rows INTEGER NOT NULL,payload_bytes INTEGER NOT NULL,receipt_rows INTEGER NOT NULL);
 INSERT OR IGNORE INTO outbox_counts SELECT 1,(SELECT count(*) FROM pending_events),(SELECT coalesce(sum(length(payload)),0) FROM pending_events),(SELECT count(*) FROM acknowledged_events) WHERE NOT EXISTS(SELECT 1 FROM outbox_counts WHERE id=1);
 CREATE TRIGGER IF NOT EXISTS pending_added AFTER INSERT ON pending_events BEGIN UPDATE outbox_counts SET pending_rows=pending_rows+1,payload_bytes=payload_bytes+length(NEW.payload) WHERE id=1; END;
 CREATE TRIGGER IF NOT EXISTS pending_removed AFTER DELETE ON pending_events BEGIN UPDATE outbox_counts SET pending_rows=pending_rows-1,payload_bytes=payload_bytes-length(OLD.payload) WHERE id=1; END;
 CREATE TRIGGER IF NOT EXISTS pending_changed AFTER UPDATE OF payload ON pending_events BEGIN UPDATE outbox_counts SET payload_bytes=payload_bytes+length(NEW.payload)-length(OLD.payload) WHERE id=1; END;
 CREATE TRIGGER IF NOT EXISTS receipt_added AFTER INSERT ON acknowledged_events BEGIN UPDATE outbox_counts SET receipt_rows=receipt_rows+1 WHERE id=1; END;
 `); err != nil {
		return err
	}
	if err := initDestinationCounts(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Migration and exact batch ACK use the same atomic move: a receipt and removal
// of its pending payload commit together, or neither becomes visible on restart.
// The events view reconstructs every original column, including nullable batch_id.
func moveReceipts(tx *sql.Tx, predicate string, args ...any) error {
	if _, err := tx.Exec("CREATE TEMP TABLE IF NOT EXISTS receipt_move_keys(identity TEXT PRIMARY KEY) WITHOUT ROWID"); err != nil {
		return err
	}
	for {
		if _, err := tx.Exec("DELETE FROM receipt_move_keys"); err != nil {
			return err
		}
		// A bounded group fits the existing drain reserve even at the 4-MiB minimum.
		// Source pages become reusable before the next group; one encompassing
		// transaction still makes the complete migration/ACK atomic.
		result, err := tx.Exec("INSERT INTO receipt_move_keys SELECT identity FROM pending_events WHERE "+predicate+" LIMIT 64", args...)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		statements := []string{
			"INSERT OR IGNORE INTO receipt_scopes(destination,instance,boot,request_id) SELECT DISTINCT destination,instance,boot,request_id FROM pending_events WHERE identity IN (SELECT identity FROM receipt_move_keys)",
			"INSERT OR IGNORE INTO receipt_batches(batch_id) SELECT DISTINCT batch_id FROM pending_events WHERE batch_id IS NOT NULL AND identity IN (SELECT identity FROM receipt_move_keys)",
			"INSERT INTO acknowledged_events(identity,digest,scope,sequence,kind,received_at,batch) SELECT e.identity,unhex(e.digest),s.id,e.sequence,e.kind,e.received_at,b.id FROM (SELECT * FROM pending_events WHERE identity IN (SELECT identity FROM receipt_move_keys)) e JOIN receipt_scopes s ON s.destination=e.destination AND s.instance=e.instance AND s.boot=e.boot AND s.request_id=e.request_id LEFT JOIN receipt_batches b ON b.batch_id=e.batch_id",
			"DELETE FROM pending_events WHERE identity IN (SELECT identity FROM receipt_move_keys)",
		}
		for _, statement := range statements {
			if _, err = tx.Exec(statement); err != nil {
				return err
			}
		}
	}
}
