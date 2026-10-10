package capture

import (
	"database/sql"
	"errors"
)

// Admission must not rescan every pending payload on the sole write connection.
// Triggers keep destination budgets exact across all writers, ACKs and rollback.
func initDestinationCounts(tx *sql.Tx) error {
	var exists int
	if err := tx.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='destination_counts'").Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		if _, err := tx.Exec(`
 CREATE TABLE destination_counts(destination TEXT PRIMARY KEY,payload_bytes INTEGER NOT NULL CHECK(payload_bytes>=0)) WITHOUT ROWID;
 INSERT INTO destination_counts SELECT destination,sum(length(payload)) FROM pending_events WHERE state='pending' GROUP BY destination;
 CREATE TRIGGER destination_pending_added AFTER INSERT ON pending_events WHEN NEW.state='pending' BEGIN
 INSERT INTO destination_counts VALUES(NEW.destination,length(NEW.payload)) ON CONFLICT(destination) DO UPDATE SET payload_bytes=payload_bytes+length(NEW.payload);
 END;
 CREATE TRIGGER destination_pending_removed AFTER DELETE ON pending_events WHEN OLD.state='pending' BEGIN
 UPDATE destination_counts SET payload_bytes=payload_bytes-length(OLD.payload) WHERE destination=OLD.destination;
 END;
 CREATE TRIGGER destination_pending_changed AFTER UPDATE OF payload,state,destination ON pending_events BEGIN
 UPDATE destination_counts SET payload_bytes=payload_bytes-length(OLD.payload) WHERE destination=OLD.destination AND OLD.state='pending';
 INSERT INTO destination_counts SELECT NEW.destination,length(NEW.payload) WHERE NEW.state='pending' ON CONFLICT(destination) DO UPDATE SET payload_bytes=payload_bytes+length(NEW.payload);
 END;
 `); err != nil {
			return err
		}
	}
	// Recount at every open, inside the same transaction. A mismatch is corruption
	// or missing bookkeeping, never permission to silently rewrite the ledger.
	var mismatches int
	if err := tx.QueryRow(`WITH actual AS (SELECT destination,sum(length(payload)) AS bytes FROM pending_events WHERE state='pending' GROUP BY destination)
 SELECT count(*) FROM (
 SELECT a.destination FROM actual a LEFT JOIN destination_counts c USING(destination) WHERE c.payload_bytes IS NULL OR c.payload_bytes!=a.bytes
 UNION ALL
 SELECT c.destination FROM destination_counts c LEFT JOIN actual a USING(destination) WHERE a.destination IS NULL AND c.payload_bytes!=0
 )`).Scan(&mismatches); err != nil {
		return err
	}
	var triggers int
	if err := tx.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN ('destination_pending_added','destination_pending_removed','destination_pending_changed')").Scan(&triggers); err != nil {
		return err
	}
	if mismatches != 0 || triggers != 3 {
		return errors.New("destination pending-byte counter mismatch; preserve outbox and investigate")
	}
	return nil
}
