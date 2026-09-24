package capture

import (
	"encoding/json"
	"strings"
)

// Header collections and opaque host metadata never enter Observation. A bound
// routing credential copied into an allowlisted scalar must not become a join
// key. Omit that scalar; never rewrite identity, diagnostic text or bodies.
func excludeCredentialMetadata(o *Observation, bindings []Binding) {
	for _, b := range bindings {
		for field, value := range map[string]*string{
			"requestedModel": &o.Model, "executionModel": &o.ExecutionModel,
			"executionProtocol": &o.ExecutionProtocol, "sourceFormat": &o.SourceFormat,
			"sourceTraceId":  &o.TraceID,
			"selectedAuthId": &o.SelectedAuthID, "selectedAuthIndex": &o.SelectedAuthIndex,
		} {
			if b.Key != "" && strings.Contains(*value, b.Key) {
				*value = ""
				o.MetadataOmissions = append(o.MetadataOmissions, field+":credential")
			}
		}
		for field, value := range o.Correlation {
			if b.Key != "" && strings.Contains(value, b.Key) {
				delete(o.Correlation, field)
				if len(o.CorrelationConflicts) < 10 {
					o.CorrelationConflicts = append(o.CorrelationConflicts, field)
				}
			}
		}
	}
}

func metadataIdentity(raw json.RawMessage, field string, o *Observation) string {
	if len(raw) == 0 {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
		o.MetadataOmissions = append(o.MetadataOmissions, field+":invalid")
		return ""
	}
	return value
}
