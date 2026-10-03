package triagev2

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const (
	ObservationSchema = "triage.observation.v1"
	AuditSchema       = "triage.audit.v1"
)

const auditRequirements = `You audit one investigation round (the validator rules, mode V1): compare the round's receipts with the tool-call index of its session observation, failed attempts included, and report findings with their locators. The round ran with the confirmed home stack %s.`

type ObservedAttempt struct {
	AttemptID   string   `json:"attempt_id"`
	Committed   bool     `json:"committed"`
	Entries     int      `json:"entries"`
	Calls       int      `json:"calls"`
	EntriesFile *string  `json:"entries_file"`
	CallsFile   *string  `json:"calls_file"`
	Gaps        []string `json:"gaps"`
}

// ObservationRecord is the Controller's audit snapshot of one round.
type ObservationRecord struct {
	Round    contract.Ref      `json:"round"`
	Attempts []ObservedAttempt `json:"attempts"`
}

type Finding struct {
	Category string     `json:"category"`
	Entry    *string    `json:"entry"`
	Receipt  *string    `json:"receipt"`
	Evidence []Evidence `json:"evidence"`
	Reason   string     `json:"reason"`
	Effect   string     `json:"effect"`
}

type Audit struct {
	Round    contract.Ref `json:"round"`
	Findings []Finding    `json:"findings"`
	Gaps     []Gap        `json:"gaps"`
}

// observed is one attempt's observation as the Step returned it.
type observed struct {
	attemptID string
	o         *engine.Observation
}

// attachObservations commits a round's observations: per attempt, the tool
// call index and a copy of the raw entries file, as far as the run's file
// limits allow; a file left out is a coverage gap, never a failure. It
// returns the entry ids an audit finding may name.
func attachObservations(ctx context.Context, r *engine.Run, key string, round contract.Ref, attempts []observed) (contract.Ref, map[string]bool, error) {
	policy := r.Snapshot().Policy
	room, slots := policy.MaxAttemptFileBytes, policy.MaxAttemptFiles
	// fits reserves room for one file, or says why it is left out.
	fits := func(size int) string {
		switch {
		case int64(size) > policy.MaxFileBytes:
			return fmt.Sprintf("%d bytes exceed the file limit of %d", size, policy.MaxFileBytes)
		case int64(size) > room || slots < 1:
			return "the round's snapshot reached the attempt file limits"
		}
		room, slots = room-int64(size), slots-1
		return ""
	}
	record := ObservationRecord{Round: round, Attempts: []ObservedAttempt{}}
	var files []contract.ControllerFile
	ids := map[string]bool{}
	// Calls indexes first: they are small and what the audit reads.
	for i, a := range attempts {
		entry := ObservedAttempt{AttemptID: a.attemptID, Committed: a.attemptID == round.AttemptID, Entries: a.o.Entries, Calls: len(a.o.Calls), Gaps: append([]string{}, a.o.Gaps...)}
		var calls bytes.Buffer
		for _, c := range a.o.Calls {
			line, err := json.Marshal(c)
			if err != nil {
				return contract.Ref{}, nil, err
			}
			calls.Write(append(line, '\n'))
			ids[c.EntryID] = true
		}
		if why := fits(calls.Len()); why != "" {
			entry.Gaps = append(entry.Gaps, "tool call index not attached: "+why)
		} else {
			id := fmt.Sprintf("calls-%d", i+1)
			entry.CallsFile = &id
			files = append(files, contract.ControllerFile{ID: id, Path: "evidence/" + id + ".jsonl", Data: calls.Bytes()})
		}
		record.Attempts = append(record.Attempts, entry)
	}
	for i, a := range attempts {
		entry := &record.Attempts[i]
		info, err := os.Stat(a.o.Path)
		if err != nil {
			entry.Gaps = append(entry.Gaps, fmt.Sprintf("entries file could not be read: %v", err))
			continue
		}
		if why := fits(int(info.Size())); why != "" {
			entry.Gaps = append(entry.Gaps, "entries file not attached: "+why)
			continue
		}
		raw, err := os.ReadFile(a.o.Path)
		if err != nil {
			entry.Gaps = append(entry.Gaps, fmt.Sprintf("entries file could not be read: %v", err))
			continue
		}
		id := fmt.Sprintf("entries-%d", i+1)
		entry.EntriesFile = &id
		files = append(files, contract.ControllerFile{ID: id, Path: "evidence/" + id + ".jsonl", Data: raw})
		for _, entryID := range entryIDs(raw) {
			ids[entryID] = true
		}
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: key, Output: contract.Spec{SchemaID: ObservationSchema}, Data: record, Files: files})
	return ref, ids, err
}

// entryIDs reads the entry ids of a recorded entries file, one
// {"source","entry"} object per line.
func entryIDs(raw []byte) []string {
	var ids []string
	lines := bufio.NewScanner(bytes.NewReader(raw))
	lines.Buffer(nil, len(raw)+1)
	for lines.Scan() {
		var line struct {
			Entry struct {
				ID string `json:"id"`
			} `json:"entry"`
		}
		if json.Unmarshal(lines.Bytes(), &line) == nil && line.Entry.ID != "" {
			ids = append(ids, line.Entry.ID)
		}
	}
	return ids
}

// sizeNote tells the auditor how much there is to read, so a part it
// could not review is reported rather than taken as clean.
func sizeNote(record ObservationRecord) string {
	var parts []string
	for _, a := range record.Attempts {
		parts = append(parts, fmt.Sprintf("%s: %d tool calls, %d entries", a.AttemptID, a.Calls, a.Entries))
	}
	return "The observation holds " + strings.Join(parts, "; ") + ". Record any part of the index you could not review as a gap."
}

// coverageNote says what the audit cannot see.
func coverageNote(record ObservationRecord) string {
	var gaps []string
	for _, a := range record.Attempts {
		for _, g := range a.Gaps {
			gaps = append(gaps, a.AttemptID+": "+g)
		}
	}
	if len(gaps) == 0 {
		return ""
	}
	return "Audit coverage is incomplete, so treat the absence of a call as unknown: " + strings.Join(gaps, "; ")
}

// checkAudit requires the exact round and a resolvable locator on every
// finding: a recorded entry, a receipt of the round, or cited evidence.
// What the findings mean is for the steward.
func checkAudit(ctx context.Context, r *engine.Run, ref, round, observation contract.Ref, receipts []Receipt, entries map[string]bool) error {
	p, err := readAccepted[Audit](ctx, r, ref, AuditSchema)
	if err != nil {
		return err
	}
	v := p.Data
	if err := sameRef("round", v.Round, round); err != nil {
		return err
	}
	in, err := citable(ctx, r, round, observation)
	if err != nil {
		return err
	}
	cite := citations{ctx, in, ref, p.Files}.check
	for i, f := range v.Findings {
		field := fmt.Sprintf("findings[%d]", i)
		if f.Entry == nil && f.Receipt == nil && len(f.Evidence) == 0 {
			return fmt.Errorf("%s: no locator; want an entry id, a receipt id or evidence", field)
		}
		if f.Entry != nil && !entries[*f.Entry] {
			return fmt.Errorf("%s.entry: got %q; want the id of an entry recorded in the session observation", field, *f.Entry)
		}
		if f.Receipt != nil && !slices.ContainsFunc(receipts, func(q Receipt) bool { return q.ID == *f.Receipt }) {
			return fmt.Errorf("%s.receipt: got %q; want the id of a receipt of the round", field, *f.Receipt)
		}
		if err := citeAll(cite, field+".evidence", f.Evidence); err != nil {
			return err
		}
	}
	return checkGaps("gaps", v.Gaps)
}

// audit is one V1 audit of a committed round.
type audit struct {
	Model       runtime.ModelSpec
	Ticket      string
	Round       int
	Ref         contract.Ref
	Data        Round
	Observation contract.Ref
	Record      ObservationRecord
	Entries     map[string]bool
	Home        string
	Retries     int
	Timeout     time.Duration
}

// runAudit has a fresh validator audit one round; a timed-out audit reruns.
func runAudit(ctx context.Context, r *engine.Run, skills Skills, a audit) (contract.Ref, Audit, []RecoveryFailure, error) {
	home := "none: no stack is confirmed"
	if a.Home != "" {
		home = fmt.Sprintf("%q", a.Home)
	}
	requirements := []string{fmt.Sprintf(auditRequirements, home), sizeNote(a.Record), citationRequirements}
	if note := coverageNote(a.Record); note != "" {
		requirements = append(requirements, note)
	}
	t := newTask(r, "audit", a.Ticket, []string{skills.Entry("validator")}, requirements...)
	t.Round, t.Citable = a.Round, []LabeledRef{{"round under audit", a.Ref}, {"session observation", a.Observation}}
	ref, failures, err := RetryInputs(ctx, r, r.Root(), fmt.Sprintf("round-%d-audit", a.Round), "audit", a.Retries, func(ctx context.Context, s *engine.Scope, retry *engine.Feedback) (contract.Ref, error) {
		return RunTaskStep(ctx, r, TaskStep{Scope: s, Model: a.Model, Stage: "audit", Key: "audit", Task: t, Schema: AuditSchema, Inputs: t.inputs(), Recovery: true, Timeout: a.Timeout, Feedback: retry,
			Validate: func(ctx context.Context, ref contract.Ref) error {
				return checkAudit(ctx, r, ref, a.Ref, a.Observation, a.Data.Receipts, a.Entries)
			}})
	}, nil)
	if err != nil {
		return ref, Audit{}, failures, err
	}
	p, err := readAccepted[Audit](ctx, r, ref, AuditSchema)
	return ref, p.Data, failures, err
}
