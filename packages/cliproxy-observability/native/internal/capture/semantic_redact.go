package capture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// String fragments are redaction state only. No model usage/output normalization occurs here.
// Hold ambiguous credential prefixes and serialized JSON arguments until safe to release.
type semanticFrame struct {
	event string
	value any
	done  bool
}
type stringSlot struct {
	value    string
	set      func(string)
	argument bool
}
type semanticRedactor struct {
	frames []semanticFrame
	bytes  int
	failed bool
}

func (s *semanticRedactor) push(event string, raw []byte, terminal bool, secrets []string) ([]byte, string) {
	if s.failed {
		return nil, "redaction_state_lost_withheld"
	}
	if raw != nil {
		frame := semanticFrame{event: event, done: string(raw) == "[DONE]"}
		if !frame.done {
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if decoder.Decode(&frame.value) != nil {
				s.failed = true
				return nil, "invalid_semantic_frame_withheld"
			}
		}
		s.frames = append(s.frames, frame)
		s.bytes += len(raw) + len(event) + 16
	}
	if s.bytes > MaxBody || len(s.frames) > 8192 {
		s.frames = nil
		s.bytes = 0
		s.failed = true
		return nil, "redaction_fragment_limit"
	}
	lanes := map[string][]stringSlot{}
	for i := range s.frames {
		frame := &s.frames[i]
		if frame.done {
			continue
		}
		root, _ := frame.value.(map[string]any)
		context := fmt.Sprint(root["type"], "/", root["item_id"], "/", root["index"], "/", root["content_index"])
		var walk func(any, string)
		walk = func(value any, path string) {
			switch x := value.(type) {
			case map[string]any:
				// Explicit native indices separate interleaved string lanes without guessing identities.
				if index, ok := x["index"]; ok {
					path += fmt.Sprint("#", index)
				}
				for key, value := range x {
					p := path + "/" + key
					if text, ok := value.(string); ok {
						// Scalar IDs/types/models are independently redacted, never concatenated.
						if key == "name" && !strings.Contains(path, "/function") {
							continue
						}
						if !strings.Contains("|text|thinking|reasoning|reasoning_content|content|refusal|signature|partial_json|arguments|delta|name|", "|"+key+"|") {
							continue
						}
						key := key
						m := x
						argument := key == "partial_json" || key == "arguments" || (key == "delta" && root["type"] == "response.function_call_arguments.delta")
						lanes[context+p] = append(lanes[context+p], stringSlot{text, func(v string) { m[key] = v }, argument})
					} else {
						walk(value, p)
					}
				}
			case []any:
				for index, value := range x {
					walk(value, fmt.Sprintf("%s/%d", path, index))
				}
			}
		}
		walk(frame.value, "")
	}
	hold := false
	gap := ""
	for _, slots := range lanes {
		var joined strings.Builder
		argument := false
		for _, slot := range slots {
			joined.WriteString(slot.value)
			argument = argument || slot.argument
		}
		value := joined.String()
		changed := false
		if argument && value != "" {
			if json.Valid([]byte(value)) {
				clean, ok := scrubJSON([]byte(value), secrets)
				if !ok {
					s.failed = true
					s.frames = nil
					s.bytes = 0
					return nil, "unredactable_arguments_withheld"
				}
				value = string(clean)
				changed = true
			} else if terminal {
				value = "[REDACTED]"
				changed = true
				gap = "incomplete_arguments_withheld"
			} else {
				hold = true
				continue
			}
		}
		for _, secret := range secrets {
			if strings.Contains(value, secret) {
				value = strings.ReplaceAll(value, secret, "[REDACTED]")
				changed = true
			}
		}
		prefix := 0
		for _, secret := range secrets {
			for n := min(len(secret)-1, len(value)); n > prefix; n-- {
				if strings.HasSuffix(value, secret[:n]) {
					prefix = n
					break
				}
			}
		}
		if prefix > 0 {
			if !terminal {
				hold = true
			} else {
				value = value[:len(value)-prefix] + "[REDACTED]"
				changed = true
			}
		}
		if changed {
			for i, slot := range slots {
				if i == len(slots)-1 {
					slot.set(value)
				} else {
					slot.set("")
				}
			}
		}
	}
	if hold {
		return nil, ""
	}
	var out []byte
	for _, frame := range s.frames {
		if frame.event != "" {
			out = append(out, []byte("event: "+frame.event+"\n")...)
		}
		data := []byte("[DONE]")
		if !frame.done {
			var err error
			data, err = json.Marshal(frame.value)
			if err != nil {
				s.failed = true
				return nil, "invalid_semantic_frame_withheld"
			}
		}
		out = append(out, []byte("data: ")...)
		out = append(out, data...)
		out = append(out, '\n', '\n')
	}
	s.frames = nil
	s.bytes = 0
	return out, gap
}
