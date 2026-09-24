// Package capture implements stock-hook observation, never provider normalization.
package capture

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strings"
)

const Version = "0.2.0"
const CapturePolicy = "hook-body-v1"
const MaxBody = 1 << 20
const MaxFrame = 2 << 20

var AuthorityHeaders = []string{"X-Meshix-Capture-Destination", "X-Meshix-Capture-Route", "X-Meshix-Capture-Revision"}
var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,199}$`)

type Binding struct {
	Key         string `json:"key"`
	Destination string `json:"destinationId"`
	Deployment  string `json:"deployment"`
	Environment string `json:"environment"`
}
type Config struct {
	Enabled    bool      `json:"enabled"`
	Instance   string    `json:"instanceId"`
	Revision   string    `json:"revision"`
	Socket     string    `json:"socket"`
	Bindings   []Binding `json:"bindings"`
	QueueBytes int       `json:"queueBytes"`
	MaxActive  int       `json:"maxActive"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	s, err := os.Stat(path)
	if err != nil {
		return c, errors.New("capture config unavailable")
	}
	if s.Mode().Perm()&0077 != 0 || s.Size() > 128<<10 {
		return c, errors.New("capture config must be private")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return c, errors.New("capture config unreadable")
	}
	if json.Unmarshal(raw, &c) != nil {
		return c, errors.New("invalid capture config")
	}
	return c, c.Validate()
}
func (c *Config) Validate() error {
	if !identifier.MatchString(c.Instance) || !identifier.MatchString(c.Revision) || !strings.HasPrefix(c.Socket, "/") {
		return errors.New("invalid capture identity/socket")
	}
	if c.QueueBytes == 0 {
		c.QueueBytes = 16 << 20
	}
	if c.MaxActive == 0 {
		c.MaxActive = 1024
	}
	if c.QueueBytes < MaxFrame || c.QueueBytes > 64<<20 || c.MaxActive < 1 || c.MaxActive > 4096 {
		return errors.New("invalid capture bounds")
	}
	if len(c.Bindings) > 8 {
		return errors.New("capture configuration count limit")
	}
	keys := map[string]bool{}
	destinations := map[string]string{}
	for _, b := range c.Bindings {
		if len(b.Key) < 16 || len(b.Key) > 512 || strings.ContainsAny(b.Key, "\r\n\t ,;") || keys[b.Key] || !identifier.MatchString(b.Destination) || !strings.HasPrefix(b.Deployment, "https://") || (b.Environment != "dev" && b.Environment != "prod") {
			return errors.New("invalid or duplicate scope binding")
		}
		if d, ok := destinations[b.Destination]; ok && d != b.Deployment {
			return errors.New("destination ownership conflict")
		}
		destinations[b.Destination] = b.Deployment
		keys[b.Key] = true
	}
	return nil
}

// ExactlyOne also rejects differently cased duplicate field names from raw RPC input.
func ExactlyOne(h http.Header, name string) (string, bool) {
	count := 0
	value := ""
	for k, vs := range h {
		if strings.EqualFold(k, name) {
			count += len(vs)
			if len(vs) == 1 {
				value = vs[0]
			}
		}
	}
	return value, count == 1 && value != "" && !strings.ContainsAny(value, "\r\n")
}
func Scope(c Config, h http.Header) (Binding, string) {
	var none Binding
	a, aok := ExactlyOne(h, "Authorization")
	k, kok := ExactlyOne(h, "X-Api-Key")
	// Presence with invalid cardinality is a conflict, not an absent carrier.
	for name, ok := range map[string]bool{"Authorization": aok, "X-Api-Key": kok} {
		for key := range h {
			if strings.EqualFold(key, name) && !ok {
				return none, "credentials_conflict"
			}
		}
	}
	if aok {
		if !strings.HasPrefix(a, "Bearer ") {
			return none, "credentials_conflict"
		}
		a = strings.TrimPrefix(a, "Bearer ")
	}
	if aok && kok && a != k {
		return none, "credentials_conflict"
	}
	if !aok {
		a = k
	}
	// Additional carriers could authenticate a different principal; do not enroll.
	for name := range h {
		if strings.EqualFold(name, "X-Goog-Api-Key") {
			return none, "credentials_conflict"
		}
	}
	var found *Binding
	for i := range c.Bindings {
		if c.Bindings[i].Key == a {
			found = &c.Bindings[i]
			break
		}
	}
	if found == nil {
		return none, "not_enrolled"
	}
	d, dok := ExactlyOne(h, AuthorityHeaders[0])
	r, rok := ExactlyOne(h, AuthorityHeaders[2])
	deployment, depok := ExactlyOne(h, "X-Meshix-Deployment")
	route, routeok := ExactlyOne(h, AuthorityHeaders[1])
	if !dok || !rok || !depok || !routeok || d != found.Destination || r != c.Revision || deployment != found.Deployment || !AllowedRoute(route) {
		return none, "scope_conflict"
	}
	return *found, ""
}
func AllowedRoute(route string) bool {
	switch route {
	case "GET /v1/models", "POST /v1/messages", "POST /v1/messages/count_tokens", "POST /v1/responses", "POST /v1/chat/completions":
		return true
	}
	return false
}
func BootID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("capture boot entropy unavailable")
	}
	return hex.EncodeToString(b)
}
func Digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

type Observation struct {
	DroppedObservations  uint64            `json:"droppedObservationsTotal"`
	ScopeConflicts       uint64            `json:"scopeConflictsTotal"`
	SchemaVersion        int               `json:"schemaVersion"`
	PluginVersion        string            `json:"pluginVersion"`
	CapturePolicy        string            `json:"capturePolicy"`
	Destination          string            `json:"destinationId"`
	Instance             string            `json:"instanceId"`
	Boot                 string            `json:"pluginBootId"`
	RequestID            string            `json:"requestId"`
	Sequence             uint64            `json:"sequence"`
	Kind                 string            `json:"kind"`
	ObservedAt           string            `json:"observedAt"`
	OffsetNS             int64             `json:"offsetNs"`
	Route                string            `json:"route"`
	Revision             string            `json:"configRevision"`
	SourceFormat         string            `json:"sourceFormat,omitempty"`
	SelectedAuthID       string            `json:"selectedAuthId,omitempty"`
	SelectedAuthIndex    string            `json:"selectedAuthIndex,omitempty"`
	ExecutionModel       string            `json:"executionModel,omitempty"`
	ExecutionProtocol    string            `json:"executionProtocol,omitempty"`
	MetadataOmissions    []string          `json:"metadataOmissions,omitempty"`
	Model                string            `json:"requestedModel,omitempty"`
	TraceID              string            `json:"sourceTraceId,omitempty"`
	Correlation          map[string]string `json:"correlation,omitempty"`
	CorrelationConflicts []string          `json:"correlationConflicts,omitempty"`
	ChunkIndex           *int              `json:"stockChunkIndex,omitempty"`
	ObservedBodyBytes    int               `json:"observedBodyBytes,omitempty"`
	BodyFraming          string            `json:"bodyFraming,omitempty"`
	Body                 []byte            `json:"body,omitempty"`
	ContentSHA256        string            `json:"contentSha256"`
	ContentBytes         int               `json:"contentBytes"`
	Outcome              string            `json:"completionOutcome,omitempty"`
	StatusCode           int               `json:"executionStatusCode,omitempty"`
	StartedAt            string            `json:"executionStartedAt,omitempty"`
	CompletedAt          string            `json:"executionCompletedAt,omitempty"`
	Error                string            `json:"error,omitempty"`
	ObservedErrorBytes   int               `json:"observedErrorBytes,omitempty"`
	ErrorPresent         bool              `json:"errorPresent,omitempty"`
	Gap                  string            `json:"gap,omitempty"`
}

func (o Observation) Identity() string {
	raw, _ := json.Marshal([]any{o.Destination, o.Instance, o.Boot, o.RequestID, o.Sequence, o.Kind})
	return Digest(raw)
}
func (o Observation) Validate() error {
	if o.SchemaVersion != 1 || !identifier.MatchString(o.Destination) || !identifier.MatchString(o.Instance) || !identifier.MatchString(o.Boot) || !identifier.MatchString(o.RequestID) || o.Sequence == 0 || !AllowedRoute(o.Route) || len(o.Body) > MaxBody || o.ContentBytes != len(o.Body) || o.ContentSHA256 != Digest(o.Body) {
		return errors.New("invalid observation")
	}
	return nil
}
