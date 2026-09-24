package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"
)

type Health struct {
	SchemaVersion     int    `json:"schemaVersion"`
	Operation         string `json:"operation"`
	Destination       string `json:"destinationId"`
	Instance          string `json:"instanceId"`
	Boot              string `json:"pluginBootId"`
	StartedAt         string `json:"startedAt"`
	ObservedAt        string `json:"observedAt"`
	Observations      uint64 `json:"observationsTotal"`
	Dropped           uint64 `json:"droppedObservationsTotal"`
	ControlLost       uint64 `json:"lostControlObservationsTotal"`
	ScopeConflicts    uint64 `json:"scopeConflictsTotal"`
	ExpiredScopes     uint64 `json:"expiredScopesTotal"`
	Active            int    `json:"activeCalls"`
	PrecommitCoverage string `json:"precommitCoverage"`
}

func (e *Engine) healthLoop(ctx context.Context) {
	defer close(e.healthDone)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", e.config.Socket)
	}, MaxConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if e.config.Enabled {
			now := time.Now()
			e.mu.Lock()
			for id, scope := range e.scopes {
				if now.Sub(scope.last) > 24*time.Hour {
					delete(e.scopes, id)
					e.expired.Add(1)
				}
			}
			active := len(e.scopes)
			e.mu.Unlock()
			seen := map[string]bool{}
			for _, binding := range e.config.Bindings {
				if seen[binding.Destination] {
					continue
				}
				seen[binding.Destination] = true
				h := Health{1, "health_record", binding.Destination, e.config.Instance, e.boot, e.started, now.UTC().Format(time.RFC3339Nano), e.observations.Load(), e.dropped.Load(), e.controlLost.Load(), e.conflicts.Load(), e.expired.Load(), active, "unknown_before_local_commit"}
				raw, _ := json.Marshal(h)
				req, _ := http.NewRequestWithContext(ctx, "POST", "http://capture/health", bytes.NewReader(raw))
				req.Header.Set("Content-Type", "application/json")
				res, err := client.Do(req)
				if err == nil {
					io.Copy(io.Discard, io.LimitReader(res.Body, 1024))
					res.Body.Close()
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (o *Outbox) admitHealth(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 16385))
	var h Health
	if err != nil || len(raw) > 16384 || json.Unmarshal(raw, &h) != nil || h.SchemaVersion != 1 || h.Operation != "health_record" || !identifier.MatchString(h.Destination) || !identifier.MatchString(h.Instance) || !identifier.MatchString(h.Boot) || h.Active < 0 || h.Active > 4096 || h.PrecommitCoverage != "unknown_before_local_commit" {
		http.Error(w, "invalid health", 400)
		return
	}
	observed, err := time.Parse(time.RFC3339Nano, h.ObservedAt)
	if err != nil {
		http.Error(w, "invalid health time", 400)
		return
	}
	if _, err = time.Parse(time.RFC3339Nano, h.StartedAt); err != nil {
		http.Error(w, "invalid boot time", 400)
		return
	}
	raw, _ = json.Marshal(h)
	// Health is content-free and retains one bounded durable row per boot/destination.
	_, err = o.db.Exec("INSERT INTO health(destination,instance,boot,observed_at,payload,delivered_digest,dirty) VALUES (?,?,?,?,?,'',1) ON CONFLICT(destination,instance,boot) DO UPDATE SET observed_at=excluded.observed_at,payload=excluded.payload,dirty=1 WHERE excluded.observed_at>=health.observed_at", h.Destination, h.Instance, h.Boot, observed.UnixMilli(), raw)
	if err != nil {
		http.Error(w, "health commit unavailable", 507)
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"committed": true, "digest": Digest(raw)})
}
func (o *Outbox) sendHealth(ctx context.Context, d Destination, client *http.Client) {
	var boot string
	var raw []byte
	var delivered string
	err := o.db.QueryRow("SELECT boot,payload,delivered_digest FROM health WHERE destination=? AND instance=? AND dirty=1 ORDER BY observed_at LIMIT 1", d.ID, d.Instance).Scan(&boot, &raw, &delivered)
	if err != nil || delivered == Digest(raw) {
		return
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", d.URL, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+d.Token)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	ackRaw, _ := io.ReadAll(io.LimitReader(res.Body, 4097))
	var ack struct {
		Committed   bool   `json:"committed"`
		Digest      string `json:"digest"`
		Destination string `json:"destinationId"`
		Deployment  string `json:"deploymentId"`
	}
	if res.StatusCode != 200 && res.StatusCode != 408 && res.StatusCode != 429 && res.StatusCode < 500 {
		o.setDeliveryStatus(d.ID, "quarantined", res.StatusCode, "health_record_rejected")
		return
	}
	if res.StatusCode == 200 {
		if len(ackRaw) > 4096 || json.Unmarshal(ackRaw, &ack) != nil || !ack.Committed || ack.Digest != Digest(raw) || ack.Destination != d.ID || ack.Deployment != d.Deployment {
			o.setDeliveryStatus(d.ID, "quarantined", 409, "health_ack_identity_failed")
			return
		}
		_, _ = o.db.Exec("UPDATE health SET delivered_digest=?,dirty=0 WHERE destination=? AND instance=? AND boot=? AND payload=?", ack.Digest, d.ID, d.Instance, boot, raw)
	}
}

func (e *Engine) controlWorker(ctx context.Context, pipe *capturePipe) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", e.config.Socket)
	}, MaxConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for {
		select {
		case <-ctx.Done():
			return
		case q := <-pipe.control:
			o := q.o
			o.DroppedObservations = e.dropped.Load()
			o.ScopeConflicts = e.conflicts.Load()
			raw, _ := json.Marshal(o)
			for {
				req, _ := http.NewRequestWithContext(ctx, "POST", "http://capture/events", bytes.NewReader(raw))
				req.Header.Set("Content-Type", "application/json")
				res, err := client.Do(req)
				accepted := false
				if err == nil {
					body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
					res.Body.Close()
					var ack struct{ Identity, Digest string }
					accepted = res.StatusCode == 200 && json.Unmarshal(body, &ack) == nil && ack.Identity == o.Identity() && ack.Digest == Digest(raw)
				}
				if accepted {
					break
				}
				if !pause(ctx, 250*time.Millisecond) {
					return
				}
			}
		}
	}
}
