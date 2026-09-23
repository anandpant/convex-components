package capture

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const MaxSegment = 1408 << 10

type Destination struct {
	ID         string `json:"destinationId"`
	Deployment string `json:"deploymentId"`
	URL        string `json:"url"`
	Token      string `json:"token"`
	Instance   string `json:"instanceId"`
}
type DeliveryConfig struct {
	Destinations []Destination `json:"destinations"`
}

func LoadDeliveryConfig(path string) (DeliveryConfig, error) {
	var c DeliveryConfig
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return c, errors.New("delivery config must exist and be private")
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) > 64<<10 {
		return c, errors.New("delivery config unavailable")
	}
	if json.Unmarshal(raw, &c) != nil {
		return c, errors.New("invalid delivery config")
	}
	return c, c.Validate()
}
func (c DeliveryConfig) Validate() error {
	if len(c.Destinations) > 8 {
		return errors.New("destination limit")
	}
	seen := map[string]bool{}
	for _, d := range c.Destinations {
		u, err := url.Parse(d.URL)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/cliproxy/capture/v1" || !identifier.MatchString(d.ID) || !identifier.MatchString(d.Instance) || d.Deployment == "" || len(d.Deployment) > 256 || len(d.Token) < 24 || strings.ContainsAny(d.Token, "\r\n") || seen[d.ID] {
			return errors.New("invalid or duplicate delivery destination")
		}
		seen[d.ID] = true
	}
	return nil
}

type SegmentEnvelope struct {
	SchemaVersion int    `json:"schemaVersion"`
	Operation     string `json:"operation"`
	Destination   string `json:"destinationId"`
	Instance      string `json:"instanceId"`
	Boot          string `json:"pluginBootId"`
	Request       string `json:"requestId"`
	First         uint64 `json:"firstSequence"`
	Through       uint64 `json:"throughSequence"`
	Digest        string `json:"contentSha256"`
	Bytes         int    `json:"contentBytes"`
	Content       string `json:"contentBase64"`
}
type deliveryBatch struct {
	Identity, Digest, Path, Destination, Instance, Boot, Request string
	First, Through                                               uint64
	Bytes, Attempts                                              int
}

func (d Destination) binding() string {
	b, _ := json.Marshal([]string{d.ID, d.Deployment, d.URL, d.Instance})
	return Digest(b)
}
func (o *Outbox) initDelivery() error {
	_, err := o.db.Exec(`CREATE TABLE IF NOT EXISTS health(destination TEXT NOT NULL,instance TEXT NOT NULL,boot TEXT NOT NULL,observed_at INTEGER NOT NULL,payload BLOB NOT NULL,delivered_digest TEXT NOT NULL,dirty INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(destination,instance,boot)); CREATE INDEX IF NOT EXISTS health_pending ON health(destination,instance,observed_at); CREATE TABLE IF NOT EXISTS delivery_bindings(destination TEXT PRIMARY KEY,binding TEXT NOT NULL); CREATE TABLE IF NOT EXISTS batches(identity TEXT PRIMARY KEY,digest TEXT NOT NULL,path TEXT NOT NULL,destination TEXT NOT NULL,instance TEXT NOT NULL,boot TEXT NOT NULL,request_id TEXT NOT NULL,first_sequence INTEGER NOT NULL,through_sequence INTEGER NOT NULL,bytes INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL DEFAULT 0,last_status INTEGER NOT NULL DEFAULT 0); CREATE TABLE IF NOT EXISTS segment_limits(destination TEXT NOT NULL,boot TEXT NOT NULL,request_id TEXT NOT NULL,max_events INTEGER NOT NULL,PRIMARY KEY(destination,boot,request_id)); CREATE TABLE IF NOT EXISTS delivery_status(destination TEXT PRIMARY KEY,state TEXT NOT NULL,last_status INTEGER NOT NULL,reason TEXT NOT NULL,observed_at INTEGER NOT NULL); CREATE INDEX IF NOT EXISTS batches_state ON batches(state); CREATE INDEX IF NOT EXISTS batches_pending ON batches(destination,state,next_attempt); CREATE INDEX IF NOT EXISTS events_pending ON events(destination,state,received_at); UPDATE batches SET state='pending' WHERE state='delivering'; UPDATE delivery_status SET state='unavailable',reason='destination_not_active_after_restart';`)
	if err != nil {
		return err
	}
	rows, err := o.db.Query("PRAGMA table_info(events)")
	if err != nil {
		return err
	}
	exists := false
	for rows.Next() {
		var n int
		var name, typ string
		var notnull, pk int
		var def any
		if err = rows.Scan(&n, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		exists = exists || name == "batch_id"
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !exists {
		_, err = o.db.Exec("ALTER TABLE events ADD COLUMN batch_id TEXT")
	}
	if err != nil {
		return err
	}
	dir := filepath.Join(filepath.Dir(o.path), "segments")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm()&0077 != 0 {
		return errors.New("segments directory must be private")
	}
	return o.reconcileSegments(dir)
}

// RunDelivery has one owner per destination. A paused or retrying receiver cannot block another.
func (o *Outbox) RunDelivery(ctx context.Context, c DeliveryConfig, client *http.Client) <-chan struct{} {
	done := make(chan struct{})
	o.destinationBudgets = map[string]int64{}
	for _, d := range c.Destinations {
		o.destinationBudgets[d.ID] = o.budget / int64(max(1, len(c.Destinations))) / 2
	}
	var wg sync.WaitGroup
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client = &copyClient
	for _, d := range c.Destinations {
		wg.Add(1)
		go func() { defer wg.Done(); o.deliverDestination(ctx, d, client) }()
	}
	go func() { wg.Wait(); close(done) }()
	return done
}
func (o *Outbox) bindDestination(d Destination) error {
	_, err := o.db.Exec("INSERT OR IGNORE INTO delivery_bindings(destination,binding) VALUES (?,?)", d.ID, d.binding())
	if err != nil {
		return err
	}
	var binding string
	if err = o.db.QueryRow("SELECT binding FROM delivery_bindings WHERE destination=?", d.ID).Scan(&binding); err != nil {
		return err
	}
	if binding != d.binding() {
		return errors.New("destination binding changed; pending data retained")
	}
	return nil
}
func (o *Outbox) health(ctx context.Context, d Destination, client *http.Client) (int, bool) {
	raw, _ := json.Marshal(map[string]any{"schemaVersion": 1, "operation": "health", "destinationId": d.ID, "instanceId": d.Instance})
	req, _ := http.NewRequestWithContext(ctx, "POST", d.URL, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+d.Token)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 4097))
	var health struct {
		Destination string `json:"destinationId"`
		Deployment  string `json:"deploymentId"`
		Instance    string `json:"instanceId"`
		Ready       bool   `json:"ready"`
	}
	ok := err == nil && len(b) <= 4096 && res.StatusCode == 200 && json.Unmarshal(b, &health) == nil && health.Ready && health.Destination == d.ID && health.Deployment == d.Deployment && health.Instance == d.Instance
	status := res.StatusCode
	if status == 200 && !ok {
		status = 409
	}
	return status, ok
}
func (o *Outbox) deliverDestination(ctx context.Context, d Destination, client *http.Client) {
	if o.bindDestination(d) != nil {
		o.setDeliveryStatus(d.ID, "quarantined", 409, "destination_binding_changed")
		return
	}
	// A deployment-authenticated, content-free check precedes every daemon boot's delivery.
	for attempts := 0; ; attempts++ {
		status, ready := o.health(ctx, d, client)
		if ready {
			break
		}
		if status != 0 && status != 408 && status != 429 && status < 500 {
			o.setDeliveryStatus(d.ID, "quarantined", status, "health_auth_or_identity_failed")
			for {
				if !pause(ctx, time.Second) {
					return
				}
				var state string
				o.db.QueryRow("SELECT state FROM delivery_status WHERE destination=?", d.ID).Scan(&state)
				if state != "quarantined" {
					break
				}
			}
		} else {
			o.setDeliveryStatus(d.ID, "unavailable", status, "health_unavailable")
			if !pause(ctx, min(5*time.Minute, time.Second*time.Duration(1<<min(attempts+1, 8)))) {
				return
			}
		}
	}
	var quarantined int
	_ = o.db.QueryRow("SELECT count(*) FROM batches WHERE destination=? AND state='quarantined'", d.ID).Scan(&quarantined)
	if quarantined > 0 {
		o.setDeliveryStatus(d.ID, "quarantined", 0, "operator_resume_required")
	} else {
		o.setDeliveryStatus(d.ID, "pending", 0, "ready")
	}
	lastHealth := time.Time{}
	for ctx.Err() == nil {
		var state string
		_ = o.db.QueryRow("SELECT state FROM delivery_status WHERE destination=?", d.ID).Scan(&state)
		if state == "quarantined" {
			if !pause(ctx, time.Second) {
				return
			}
			continue
		}
		if time.Since(lastHealth) >= 2*time.Second {
			o.sendHealth(ctx, d, client)
			lastHealth = time.Now()
			o.db.QueryRow("SELECT state FROM delivery_status WHERE destination=?", d.ID).Scan(&state)
			if state == "quarantined" {
				continue
			}
		}
		batch, err := o.nextBatch(d)
		if err != nil || batch == nil {
			if !pause(ctx, time.Second) {
				return
			}
			continue
		}
		status, retryAfter, accepted := o.sendBatch(ctx, d, *batch, client)
		if accepted {
			if o.ackBatch(*batch) != nil {
				_, _ = o.db.Exec("UPDATE batches SET state='pending' WHERE identity=? AND state!='delivered'", batch.Identity)
				o.setDeliveryStatus(d.ID, "unavailable", 0, "local_commit_or_replica_cleanup_failed")
				pause(ctx, time.Second)
			} else {
				o.setDeliveryStatus(d.ID, "delivering", 200, "raw_committed")
			}
			continue
		}
		if status == 413 && o.resegment(*batch) == nil {
			continue
		}
		state = "pending"
		if status != 0 && status != 408 && status != 429 && status < 500 {
			state = "quarantined"
		}
		o.setDeliveryStatus(d.ID, state, status, "delivery_retry_or_quarantine")
		attempts := batch.Attempts + 1
		delay := time.Second * time.Duration(1<<min(attempts, 8))
		delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		if retryAfter > delay {
			delay = retryAfter
		}
		delay = min(delay, 5*time.Minute)
		_, _ = o.db.Exec("UPDATE batches SET state=?,attempts=?,next_attempt=?,last_status=? WHERE identity=?", state, attempts, time.Now().Add(delay).UnixMilli(), status, batch.Identity)
	}
}
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (o *Outbox) nextBatch(d Destination) (*deliveryBatch, error) {
	var b deliveryBatch
	err := o.db.QueryRow("SELECT identity,digest,path,destination,instance,boot,request_id,first_sequence,through_sequence,bytes,attempts FROM batches WHERE destination=? AND state='pending' AND next_attempt<=? ORDER BY next_attempt,first_sequence LIMIT 1", d.ID, time.Now().UnixMilli()).Scan(&b.Identity, &b.Digest, &b.Path, &b.Destination, &b.Instance, &b.Boot, &b.Request, &b.First, &b.Through, &b.Bytes, &b.Attempts)
	if err == nil {
		return &b, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}
	// Preserve every persisted batch boundary across retry/restart. Never overlap a batch.
	tx, err := o.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var firstPayload []byte
	err = tx.QueryRow("SELECT payload FROM events WHERE destination=? AND instance=? AND state='pending' AND batch_id IS NULL ORDER BY received_at,request_id,sequence LIMIT 1", d.ID, d.Instance).Scan(&firstPayload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var first Observation
	if json.Unmarshal(firstPayload, &first) != nil {
		return nil, errors.New("invalid stored event")
	}
	limit := 256
	_ = tx.QueryRow("SELECT max_events FROM segment_limits WHERE destination=? AND boot=? AND request_id=?", d.ID, first.Boot, first.RequestID).Scan(&limit)
	rows, err := tx.Query("SELECT identity,payload,sequence FROM events WHERE destination=? AND instance=? AND boot=? AND request_id=? AND state='pending' AND batch_id IS NULL AND sequence>=? ORDER BY sequence LIMIT 256", d.ID, d.Instance, first.Boot, first.RequestID, first.Sequence)
	if err != nil {
		return nil, err
	}
	content := []byte{}
	ids := []string{}
	through := first.Sequence - 1
	for rows.Next() {
		var id string
		var raw []byte
		var sequence uint64
		if err = rows.Scan(&id, &raw, &sequence); err != nil {
			rows.Close()
			return nil, err
		}
		if len(ids) >= limit || sequence != through+1 || len(content)+len(raw)+1 > MaxSegment {
			break
		}
		content = append(content, raw...)
		content = append(content, '\n')
		ids = append(ids, id)
		through = sequence
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, errors.New("event exceeds supported segment bound")
	}
	identityRaw, _ := json.Marshal([]any{d.ID, d.Instance, first.Boot, first.RequestID, first.Sequence, through})
	digest := Digest(content)
	path := filepath.Join(filepath.Dir(o.path), "segments", digest+".ndjson")
	if !o.segmentCapacity(tx, int64(len(content))) {
		return nil, errors.New("segment disk budget")
	}
	if err = durableSegment(path, content); err != nil {
		return nil, err
	}
	b = deliveryBatch{Identity: Digest(identityRaw), Digest: digest, Path: path, Destination: d.ID, Instance: d.Instance, Boot: first.Boot, Request: first.RequestID, First: first.Sequence, Through: through, Bytes: len(content)}
	_, err = tx.Exec("INSERT INTO batches(identity,digest,path,destination,instance,boot,request_id,first_sequence,through_sequence,bytes) VALUES (?,?,?,?,?,?,?,?,?,?)", b.Identity, b.Digest, b.Path, b.Destination, b.Instance, b.Boot, b.Request, b.First, b.Through, b.Bytes)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err = tx.Exec("UPDATE events SET batch_id=? WHERE identity=?", b.Identity, id); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &b, nil
}
func durableSegment(path string, content []byte) error {
	if raw, err := os.ReadFile(path); err == nil {
		if Digest(raw) != Digest(content) {
			return errors.New("segment digest conflict")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".staging-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(content); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func (o *Outbox) sendBatch(ctx context.Context, d Destination, b deliveryBatch, client *http.Client) (int, time.Duration, bool) {
	raw, err := os.ReadFile(b.Path)
	if err != nil || len(raw) != b.Bytes || Digest(raw) != b.Digest {
		return 409, 0, false
	}
	body, _ := json.Marshal(SegmentEnvelope{1, "segment", b.Destination, b.Instance, b.Boot, b.Request, b.First, b.Through, b.Digest, len(raw), base64.StdEncoding.EncodeToString(raw)})
	if len(body) > MaxFrame {
		return 413, 0, false
	}
	_, err = o.db.Exec("UPDATE batches SET state='delivering' WHERE identity=?", b.Identity)
	if err != nil {
		return 0, 0, false
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", d.URL, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+d.Token)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return 0, 0, false
	}
	defer res.Body.Close()
	ackRaw, err := io.ReadAll(io.LimitReader(res.Body, 4097))
	var ack struct {
		Identity, Digest, CallID    string
		DestinationID, DeploymentID string
		RawCommitted                bool
	}
	callRaw, _ := json.Marshal([]string{b.Destination, b.Instance, b.Boot, b.Request})
	accepted := err == nil && len(ackRaw) <= 4096 && res.StatusCode == 200 && json.Unmarshal(ackRaw, &ack) == nil && ack.Identity == b.Identity && ack.Digest == b.Digest && ack.CallID == Digest(callRaw) && ack.RawCommitted && ack.DestinationID == d.ID && ack.DeploymentID == d.Deployment
	status := res.StatusCode
	if status == 200 && !accepted {
		status = 409
	}
	retry := time.Duration(0)
	if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds > 0 {
		retry = min(time.Duration(seconds)*time.Second, 5*time.Minute)
	}
	return status, retry, accepted
}
func (o *Outbox) ackBatch(b deliveryBatch) error {
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("UPDATE events SET state='delivered',payload=x'' WHERE batch_id=?", b.Identity); err != nil {
		return err
	}
	if _, err = tx.Exec("UPDATE batches SET state='delivered',last_status=200 WHERE identity=?", b.Identity); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	_, _ = o.db.Exec("PRAGMA incremental_vacuum(256)")
	_, _ = o.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return os.Remove(b.Path)
}

func (o *Outbox) setDeliveryStatus(destination, state string, status int, reason string) {
	_, _ = o.db.Exec("INSERT INTO delivery_status(destination,state,last_status,reason,observed_at) VALUES (?,?,?,?,?) ON CONFLICT(destination) DO UPDATE SET state=excluded.state,last_status=excluded.last_status,reason=excluded.reason,observed_at=excluded.observed_at", destination, state, status, reason, time.Now().UnixMilli())
}
func (o *Outbox) deliveryStatus() []map[string]any {
	rows, err := o.db.Query("SELECT destination,state,last_status,reason,observed_at FROM delivery_status ORDER BY destination LIMIT 8")
	if err != nil {
		return nil
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		var d, s, r string
		var code int
		var at int64
		if rows.Scan(&d, &s, &code, &r, &at) == nil {
			result = append(result, map[string]any{"destinationId": d, "state": s, "lastStatus": code, "reason": r, "observedAt": at})
		}
	}
	return result
}
func (o *Outbox) segmentCapacity(tx *sql.Tx, extra int64) bool {
	var segments int64
	if tx.QueryRow("SELECT coalesce(sum(bytes),0) FROM batches WHERE state IN ('pending','delivering','quarantined')").Scan(&segments) != nil {
		return false
	}
	total := segments + extra
	for _, path := range []string{o.path, o.path + "-wal", o.path + "-shm"} {
		if info, err := os.Stat(path); err == nil {
			total += info.Size()
		}
	}
	var fs syscall.Statfs_t
	headroom := min(o.budget/8, 8<<20)
	return total+headroom <= o.budget && syscall.Statfs(filepath.Dir(o.path), &fs) == nil && uint64(fs.Bavail)*uint64(fs.Bsize) > o.reserve+uint64(extra)+uint64(headroom)
}
func (o *Outbox) reconcileSegments(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	for {
		names, err := handle.Readdirnames(128)
		for _, name := range names {
			if !strings.HasPrefix(name, ".staging-") && !strings.HasSuffix(name, ".ndjson") {
				continue
			}
			path := filepath.Join(dir, name)
			var count int
			if err := o.db.QueryRow("SELECT count(*) FROM batches WHERE path=? AND state IN ('pending','delivering','quarantined')", path).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
func (o *Outbox) resegment(b deliveryBatch) error {
	if b.First == b.Through {
		return errors.New("single observation cannot be resegmented")
	}
	// Supersede the unsent range and persist a smaller range before another attempt.
	// Event identities/content are unchanged; a 413 is never treated as a receipt.
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("UPDATE batches SET state='superseded' WHERE identity=?", b.Identity); err != nil {
		return err
	}
	if _, err = tx.Exec("UPDATE events SET batch_id=NULL WHERE batch_id=?", b.Identity); err != nil {
		return err
	}
	if _, err = tx.Exec("INSERT OR REPLACE INTO segment_limits(destination,boot,request_id,max_events) VALUES (?,?,?,?)", b.Destination, b.Boot, b.Request, max(1, int(b.Through-b.First+1)/2)); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return os.Remove(b.Path)
}
