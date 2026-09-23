package capture

import (
	"bytes"
	"encoding/json"
	"strings"
)

// FrameRedactor releases only complete JSON documents or complete SSE frames.
// Incomplete/malformed/oversized payloads are withheld with explicit gaps, never spooled raw.
// It frames bytes; receiver TypeScript remains the only protocol/usage normalizer.
type FrameRedactor struct {
	semantic         semanticRedactor
	pending          []byte
	first            uint64
	allowUndelimited bool
	allowJSON        bool
}

var sensitive = map[string]bool{"authorization": true, "api_key": true, "apikey": true, "api-key": true, "x-api-key": true, "access_token": true, "refresh_token": true, "id_token": true, "cookie": true, "set-cookie": true, "password": true, "secret": true, "token": true}

func scrubJSON(b []byte, secrets []string) ([]byte, bool) { return scrubJSONDepth(b, secrets, 0) }
func scrubJSONDepth(b []byte, secrets []string, depth int) ([]byte, bool) {
	if depth > 32 {
		return nil, false
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if d.Decode(&v) != nil || !json.Valid(b) {
		return nil, false
	}
	valid := true
	var walk func(any, int) any
	walk = func(v any, level int) any {
		if level > 32 {
			valid = false
			return nil
		}
		switch x := v.(type) {
		case map[string]any:
			for k, a := range x {
				// Unknown structural fields can still contain configured secrets in their names.
				for _, secret := range secrets {
					if strings.Contains(k, secret) {
						valid = false
						return nil
					}
				}
				if sensitive[strings.ToLower(k)] {
					x[k] = "[REDACTED]"
				} else {
					x[k] = walk(a, level+1)
				}
			}
		case []any:
			for i, a := range x {
				x[i] = walk(a, level+1)
			}
		case string:
			if json.Valid([]byte(x)) && (strings.HasPrefix(strings.TrimSpace(x), "{") || strings.HasPrefix(strings.TrimSpace(x), "[")) {
				nested, ok := scrubJSONDepth([]byte(x), secrets, level+1)
				if !ok {
					valid = false
					return nil
				}
				x = string(nested)
			}
			for _, secret := range secrets {
				x = strings.ReplaceAll(x, secret, "[REDACTED]")
			}
			return x
		}
		return v
	}
	clean := walk(v, depth)
	if !valid {
		return nil, false
	}
	out, err := json.Marshal(clean)
	return out, err == nil
}
func (r *FrameRedactor) Feed(body []byte, seq uint64, stream, terminal bool, secrets []string) ([]byte, *uint64, string) {
	if r.bufferedBytes() == 0 {
		r.first = seq
	}
	if len(r.pending)+len(body) > MaxBody {
		r.pending = nil
		return nil, nil, "redaction_frame_limit"
	}
	// Stock OpenAI->OpenAI translation strips the SSE prefix and DONE before the hook.
	// Preserve the observed chunk boundary, adding only canonical framing for transport.
	if stream && r.allowJSON && len(r.pending) == 0 && len(body) > 0 && body[0] == '{' && json.Valid(body) {
		clean, ok := scrubJSON(body, secrets)
		if !ok {
			return nil, nil, "invalid_or_unredactable_json_withheld"
		}
		first := r.first
		out, gap := r.semantic.push("", clean, terminal, secrets)
		if len(r.semantic.frames) == 0 {
			r.first = seq + 1
		}
		return out, &first, gap
	}
	r.pending = append(r.pending, body...)
	if !stream {
		if len(r.pending) == 0 {
			return nil, nil, ""
		}
		b, ok := scrubJSON(r.pending, secrets)
		r.pending = nil
		if !ok {
			return nil, nil, "invalid_or_unredactable_json_withheld"
		}
		return b, nil, ""
	}
	var out []byte
	first := r.first
	gap := ""
	for {
		end := bytes.Index(r.pending, []byte("\n\n"))
		delimiter := 2
		if cr := bytes.Index(r.pending, []byte("\r\n\r\n")); cr >= 0 && (end < 0 || cr < end) {
			end = cr
			delimiter = 4
		}
		if end < 0 {
			if r.allowUndelimited && completeCandidate(r.pending) {
				r.pending = append(r.pending, '\n', '\n')
				end = len(r.pending) - 2
				delimiter = 2
			} else {
				break
			}
		}
		frame := r.pending[:end]
		r.pending = r.pending[end+delimiter:]
		var data []string
		var event string
		valid := true
		for _, line := range strings.Split(strings.ReplaceAll(string(frame), "\r\n", "\n"), "\n") {
			if strings.HasPrefix(line, "data:") {
				data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			} else if strings.HasPrefix(line, "event:") {
				e := strings.TrimSpace(strings.TrimPrefix(line, "event:"))
				if len(e) < 128 && identifier.MatchString(e) {
					event = e
				} else {
					valid = false
				}
			}
			// Comments, retry and IDs are unnecessary and may contain credentials.
		}
		if !valid {
			gap = "invalid_or_unredactable_sse_withheld"
			continue
		}
		if len(data) == 0 {
			continue
		}
		raw := []byte(strings.Join(data, "\n"))
		clean := raw
		ok := true
		if string(raw) != "[DONE]" {
			clean, ok = scrubJSON(raw, secrets)
		}
		if !ok {
			gap = "invalid_or_unredactable_sse_withheld"
			continue
		}
		safe, reason := r.semantic.push(event, clean, false, secrets)
		if reason != "" {
			gap = reason
		}
		out = append(out, safe...)
	}
	if len(r.pending) == 0 && len(r.semantic.frames) == 0 {
		r.pending = nil
		r.first = seq + 1
	} else {
		r.pending = bytes.Clone(r.pending)
		if len(out) > 0 {
			r.first = seq
		}
	}
	if terminal {
		safe, reason := r.semantic.push("", nil, true, secrets)
		out = append(out, safe...)
		if reason != "" {
			gap = reason
		}
	}
	if terminal && len(r.pending) > 0 {
		gap = "truncated_frame_withheld"
		r.pending = nil
	}
	if len(out) > MaxBody {
		return nil, nil, "redacted_body_limit"
	}
	return out, &first, gap
}

// Responses hooks precede the stock SSE framer; complete candidate events can lack a delimiter.
func completeCandidate(frame []byte) bool {
	var data []string
	for _, line := range strings.Split(strings.ReplaceAll(string(frame), "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		} else if line != "" && !strings.HasPrefix(line, "event:") && !strings.HasPrefix(line, ":") {
			return false
		}
	}
	raw := []byte(strings.Join(data, "\n"))
	return len(data) > 0 && (json.Valid(raw) || string(raw) == "[DONE]")
}

func (r *FrameRedactor) bufferedBytes() int { return len(r.pending) + r.semantic.bytes }

func redactMetadata(o *Observation, secrets []string) {
	conflict := func(field string) {
		for _, existing := range o.CorrelationConflicts {
			if existing == field {
				return
			}
		}
		if len(o.CorrelationConflicts) < 10 {
			o.CorrelationConflicts = append(o.CorrelationConflicts, field)
		}
	}
	for _, secret := range secrets {
		o.Model = strings.ReplaceAll(o.Model, secret, "[REDACTED]")
		if strings.Contains(o.TraceID, secret) {
			o.TraceID = ""
			conflict("redacted:sourceTraceId")
		}
		for field, value := range o.Correlation {
			if strings.Contains(value, secret) {
				delete(o.Correlation, field)
				conflict("redacted:" + field)
			}
		}
	}
}
