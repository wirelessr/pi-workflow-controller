// Package protocol is a subprocess-only Pi RPC fixture shared by tests.
// Production packages must not import it.
package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"pi-workflow-controller/internal/contract"
)

type Control struct {
	Type          string
	PID           int
	History       string
	SessionID     string
	RequestPath   string
	CandidatePath string
}

type Data struct {
	Value  string `json:"value"`
	Source string `json:"source"`
}

func Serve() error {
	arg := func(key string) string {
		for i := 0; i+1 < len(os.Args); i++ {
			if os.Args[i] == key {
				return os.Args[i+1]
			}
		}
		return ""
	}
	conn, err := net.DialTimeout("tcp", os.Getenv("PWC_ENGINE_CONTROL"), 5*time.Second)
	if err != nil {
		return err
	}
	defer func(conn net.Conn) { _ = conn.Close() }(conn)
	controlOut := json.NewEncoder(conn)
	acks := make(chan Control)
	// The control socket is a fixture-owned lifeline. If an assertion kills the
	// controller, EOF stops this child without signalling any numeric PID.
	go func() {
		in := json.NewDecoder(conn)
		for {
			var ack Control
			if err := in.Decode(&ack); err != nil {
				os.Exit(0)
			}
			acks <- ack
		}
	}()
	sid := "engine-protocol-" + filepath.Base(filepath.Dir(arg("--session-dir")))
	history := filepath.Join(arg("--session-dir"), "history.jsonl")
	discovery, err := json.Marshal(map[string]any{"sessionId": sid, "sessionFile": history, "pid": os.Getppid(), "piPid": os.Getpid()})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(os.Getenv("PI_BRIDGE_DIR"), sid+".json"), discovery, 0600); err != nil {
		return err
	}
	file, err := os.OpenFile(history, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func(file *os.File) { _ = file.Close() }(file)
	historyOut := json.NewEncoder(file)
	if err := controlOut.Encode(Control{Type: "hello", PID: os.Getpid(), History: history, SessionID: sid}); err != nil {
		return err
	}
	in, out := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	provider, model, thinking := arg("--provider"), arg("--model"), arg("--thinking")
	state := map[string]any{"sessionId": sid, "sessionFile": history, "model": map[string]any{"provider": provider, "id": model}, "thinkingLevel": thinking, "isStreaming": false, "isCompacting": false, "pendingMessageCount": 0}
	entries := []map[string]any{}
	var leaf any
	holdEntries := false
	appendMessage := func(message map[string]any) error {
		id := fmt.Sprintf("e%d", len(entries)+1)
		entry := map[string]any{"id": id, "parentId": leaf, "type": "message", "message": message}
		entries = append(entries, entry)
		leaf = id
		if err := historyOut.Encode(entry); err != nil {
			return err
		}
		return out.Encode(map[string]any{"type": "message_end", "message": message})
	}
	for {
		var command struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Message string `json:"message"`
			Since   string `json:"since"`
		}
		if err := in.Decode(&command); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		reply := func(data any) error {
			return out.Encode(map[string]any{"type": "response", "id": command.ID, "command": command.Type, "success": true, "data": data})
		}
		switch command.Type {
		case "get_state":
			err = reply(state)
		case "get_session_stats":
			err = reply(map[string]any{"sessionId": sid, "sessionFile": history, "contextUsage": map[string]any{"tokens": 95000, "contextWindow": 100000, "percent": 95}})
		case "get_entries":
			if holdEntries {
				if err := controlOut.Encode(Control{Type: "entries-held"}); err != nil {
					return err
				}
				continue
			}
			start := 0
			if command.Since != "" {
				found := false
				for i, entry := range entries {
					if entry["id"] == command.Since {
						start, found = i+1, true
						break
					}
				}
				if !found {
					return fmt.Errorf("unknown append cursor %q", command.Since)
				}
			}
			err = reply(map[string]any{"entries": entries[start:], "leafId": leaf})
		case "prompt":
			holdAck := os.Getenv("PWC_ENGINE_HOLD_ACK") == "true"
			if !holdAck {
				if err = reply(nil); err != nil {
					return err
				}
			}
			state["isStreaming"] = true
			if err = out.Encode(map[string]any{"type": "agent_start"}); err != nil {
				return err
			}
			if err = appendMessage(map[string]any{"role": "user", "content": command.Message, "timestamp": len(entries) + 1}); err != nil {
				return err
			}
			var requestPath, candidatePath string
			if os.Getenv("PWC_ENGINE_MANUAL_CANDIDATE") == "1" {
				requestPath, candidatePath, _, err = ParseDispatch(command.Message)
			} else {
				requestPath, candidatePath, err = WriteCandidate(command.Message)
			}
			if err != nil {
				return err
			}
			if err = controlOut.Encode(Control{Type: "prompt", RequestPath: requestPath, CandidatePath: candidatePath}); err != nil {
				return err
			}
			ack := <-acks
			switch ack.Type {
			case "settle", "provider-error":
				stop := "stop"
				if ack.Type == "provider-error" {
					stop = "error"
				}
				assistant := map[string]any{"role": "assistant", "provider": provider, "model": model, "stopReason": stop, "content": []any{map[string]any{"type": "text", "text": "candidate written"}}, "timestamp": len(entries) + 1}
				if stop == "error" {
					assistant["errorMessage"] = "fixture provider failure"
				}
				if err = appendMessage(assistant); err != nil {
					return err
				}
				state["isStreaming"] = false
				err = out.Encode(map[string]any{"type": "agent_settled"})
			case "hold", "hold-entries":
				holdEntries = ack.Type == "hold-entries"
				if holdAck {
					if err = reply(nil); err != nil {
						return err
					}
				}
				// Continue serving RPC so cancellation must use abort, not a timeout.
				err = controlOut.Encode(Control{Type: "held"})
			default:
				return fmt.Errorf("unexpected prompt barrier ack %q", ack.Type)
			}
		case "abort", "abort_bash":
			if command.Type == "abort" {
				state["isStreaming"] = false
			}
			err = reply(nil)
		default:
			return fmt.Errorf("unexpected RPC command %q", command.Type)
		}
		if err != nil {
			return err
		}
	}
}

func ParseDispatch(message string) (string, string, contract.Request, error) {
	var requestPath, candidatePath string
	var request contract.Request
	for _, line := range strings.Split(message, "\n") {
		if path, ok := strings.CutPrefix(line, "Read request JSON: "); ok {
			requestPath = path
		}
		if _, path, ok := strings.Cut(line, "Write the complete envelope with exactly request.identity and output.schema_id to: "); ok {
			candidatePath = path
		}
	}
	if !filepath.IsAbs(requestPath) || !filepath.IsAbs(candidatePath) {
		return "", "", request, fmt.Errorf("missing absolute request/candidate paths in engine envelope")
	}
	if err := ReadJSON(requestPath, &request); err != nil {
		return "", "", request, err
	}
	if !strings.HasPrefix(message, "Controller dispatch "+request.Identity.DispatchToken+"\n") {
		return "", "", request, fmt.Errorf("dispatch token differs from request identity")
	}
	return requestPath, candidatePath, request, nil
}

func WriteCandidate(message string) (string, string, error) {
	requestPath, candidatePath, request, err := ParseDispatch(message)
	if err != nil {
		return "", "", err
	}
	resources := []contract.SchemaResource{request.Output.Schema, request.Output.Envelope}
	for _, resource := range request.Output.Resources {
		resources = append(resources, resource)
	}
	for _, resource := range resources {
		raw, err := os.ReadFile(resource.Path)
		if err != nil {
			return "", "", err
		}
		hash := sha256.Sum256(raw)
		if hex.EncodeToString(hash[:]) != resource.SHA256 || !json.Valid(raw) {
			return "", "", fmt.Errorf("invalid schema resource %q", resource.Path)
		}
	}
	data := Data{Value: request.Prompt}
	if len(request.Inputs) > 0 {
		if len(request.Inputs) != 1 {
			return "", "", fmt.Errorf("fixture expects one upstream Ref")
		}
		ref := request.Inputs[0]
		raw, err := os.ReadFile(ref.Path)
		if err != nil {
			return "", "", err
		}
		hash := sha256.Sum256(raw)
		if hex.EncodeToString(hash[:]) != ref.SHA256 {
			return "", "", fmt.Errorf("upstream Ref digest mismatch")
		}
		var previous struct {
			Data Data `json:"data"`
		}
		if err := json.Unmarshal(raw, &previous); err != nil {
			return "", "", err
		}
		data = Data{Value: previous.Data.Value + "/second", Source: ref.AttemptID}
	}
	meta := struct {
		contract.Identity
		Version  int    `json:"version"`
		SchemaID string `json:"schema_id"`
	}{request.Identity, 1, request.Output.SchemaID}
	raw, err := json.Marshal(map[string]any{"meta": meta, "data": data, "files": []any{}})
	if err == nil {
		err = os.WriteFile(candidatePath, raw, 0600)
	}
	return requestPath, candidatePath, err
}

func ReadJSON(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}
