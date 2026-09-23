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
	pending          []byte
	first            uint64
	allowUndelimited bool
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
	var walk func(any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case map[string]any:
			for k, a := range x {
				if sensitive[strings.ToLower(k)] {
					x[k] = "[REDACTED]"
				} else {
					x[k] = walk(a)
				}
			}
		case []any:
			for i, a := range x {
				x[i] = walk(a)
			}
		case string:
			// Tool arguments and nested serialized JSON can themselves contain credentials.
			if json.Valid([]byte(x)) && (strings.HasPrefix(strings.TrimSpace(x), "{") || strings.HasPrefix(strings.TrimSpace(x), "[")) {
				if nested, ok := scrubJSONDepth([]byte(x), secrets, depth+1); ok {
					x = string(nested)
				}
			}
			for _, s := range secrets {
				x = strings.ReplaceAll(x, s, "[REDACTED]")
			}
			return x
		}
		return v
	}
	out, err := json.Marshal(walk(v))
	return out, err == nil
}
func (r *FrameRedactor) Feed(body []byte, seq uint64, stream, terminal bool, secrets []string) ([]byte, *uint64, string) {
	if len(r.pending) == 0 {
		r.first = seq
	}
	if len(r.pending)+len(body) > MaxBody {
		r.pending = nil
		return nil, nil, "redaction_frame_limit"
	}
	r.pending = append(r.pending, body...)
	if !stream {
		if len(r.pending) == 0 {
			return nil, nil, ""
		}
		b, ok := scrubJSON(r.pending, secrets)
		r.pending = nil
		if !ok {
			return nil, nil, "malformed_json_withheld"
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
			gap = "malformed_sse_withheld"
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
			gap = "malformed_sse_withheld"
			continue
		}
		if event != "" {
			out = append(out, []byte("event: "+event+"\n")...)
		}
		out = append(out, []byte("data: ")...)
		out = append(out, clean...)
		out = append(out, '\n', '\n')
	}
	if len(r.pending) == 0 {
		r.pending = nil
		r.first = seq + 1
	} else {
		r.pending = bytes.Clone(r.pending)
		if len(out) > 0 {
			r.first = seq
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
