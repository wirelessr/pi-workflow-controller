// Package triage implements the unregistered intake-to-context slice. It does
// not yet implement investigation, final delivery, or a live launch path.
package triage

import "pi-workflow-controller/internal/contract"

const (
	IntakeSchema  = "triage.intake.v1"
	WikiSchema    = "triage.wiki.v1"
	ContextSchema = "triage.context.v1"
)

// Scope is supplied by the caller, never inferred from ticket/attachment text.
// Binding is a local target reference, not a credential or executable command.
type Scope struct {
	Ticket    string   `json:"ticket"`
	Stack     string   `json:"stack"`
	Pop       string   `json:"pop"`
	Binding   string   `json:"binding"`
	TenantIDs []string `json:"tenant_ids"`
}

type Source struct {
	Ref    *contract.Ref `json:"ref,omitempty"`
	Status string        `json:"status"`
	FileID string        `json:"file_id"`
	Reason string        `json:"reason"`
}

type CommentPage struct {
	Start  int    `json:"start"`
	Source Source `json:"source"`
}

type LinkedIssue struct {
	Key    string `json:"key"`
	Source Source `json:"source"`
}

type Attachment struct {
	ID       string `json:"id"`
	Content  Source `json:"content"`
	Analysis Source `json:"analysis"`
}

// intakeWork records source work, not commands or applicability rules. In a
// scoped refresh it is dispatched by the caller; in an update the agent reports
// the acquired, changed or removed slots and reasons after doing the work.
type intakeWork struct {
	Source string `json:"source"`
	Reason string `json:"reason"`
}

type Intake struct {
	Previous    *contract.Ref `json:"previous,omitempty"`
	Update      bool          `json:"update,omitempty"`
	Work        []intakeWork  `json:"work,omitempty"`
	Acquisition *Source       `json:"acquisition,omitempty"`
	Ticket      string        `json:"ticket"`
	URL         string        `json:"url"`
	FetchedAt   string        `json:"fetched_at"`
	Issue       Source        `json:"issue"`
	Fields      Source        `json:"fields"`
	Comments    []CommentPage `json:"comments"`
	Linked      []LinkedIssue `json:"linked"`
	Attachments []Attachment  `json:"attachments"`
	Complete    bool          `json:"complete"`
	Gaps        []string      `json:"gaps"`
}

// Evidence refers to an exact committed input; nil Ref means this contract's
// own file. Neither form accepts a host path or a foreign attempt's file entry.
type Evidence struct {
	Ref    *contract.Ref `json:"ref"`
	FileID string        `json:"file_id"`
}

type WikiSearch struct {
	Intake  contract.Ref `json:"intake"`
	Queries []string     `json:"queries"`
	Scope   string       `json:"scope"`
	Status  string       `json:"status"`
	Pages   []Source     `json:"pages"`
	Search  Source       `json:"search"`
	Gaps    []string     `json:"gaps"`
}

type Fact struct {
	Value    string     `json:"value"`
	Evidence []Evidence `json:"evidence"`
}

type Identity struct {
	Lookup   *Evidence `json:"lookup"`
	Status   string    `json:"status"`
	Stack    Fact      `json:"stack"`
	Pop      Fact      `json:"pop"`
	Binding  Fact      `json:"binding"`
	TenantID Fact      `json:"tenant_id"`
	OrgKey   Fact      `json:"orgkey"`
	UserKey  Fact      `json:"userkey"`
	Release  Fact      `json:"release"`
}

// A local timestamp may only be normalized using a same-event absolute anchor.
// OffsetSeconds is the calculated offset, not an assumed geographical zone.
type TimeAnchor struct {
	Event             string    `json:"event"`
	Original          string    `json:"original"`
	Format            string    `json:"format"`
	SourceTZ          string    `json:"source_tz"`
	UTC               string    `json:"utc"`
	OffsetSeconds     int       `json:"offset_seconds"`
	Evidence          Evidence  `json:"evidence"`
	PairedEpochMillis *int64    `json:"paired_epoch_millis"`
	PairedEvidence    *Evidence `json:"paired_evidence"`
}

// IdentityLookup is the normalized read-only target/DB resolution receipt.
// Preserve the original query/response alongside it; this is not root-cause proof.
type IdentityLookup struct {
	Stack   string           `json:"stack"`
	Pop     string           `json:"pop"`
	Binding string           `json:"binding"`
	Release string           `json:"release"`
	Matches []TenantIdentity `json:"matches"`
}

type TenantIdentity struct {
	TenantID string `json:"tenant_id"`
	OrgKey   string `json:"orgkey"`
}

type TimeResolution struct {
	Status  string       `json:"status"`
	From    string       `json:"from"`
	To      string       `json:"to"`
	Anchors []TimeAnchor `json:"anchors"`
}

type ResolutionAttempt struct {
	Kind     string     `json:"kind"`
	Source   string     `json:"source"`
	Outcome  string     `json:"outcome"`
	Evidence []Evidence `json:"evidence"`
}

type Context struct {
	Previous           *contract.Ref       `json:"previous,omitempty"`
	ResolvedGaps       []Fact              `json:"resolved_gaps,omitempty"`
	Intake             contract.Ref        `json:"intake"`
	Wiki               contract.Ref        `json:"wiki"`
	Scope              Scope               `json:"scope"`
	Problem            string              `json:"problem"`
	Identity           Identity            `json:"identity"`
	Time               TimeResolution      `json:"time"`
	Observations       []Fact              `json:"observations"`
	Attempts           []ResolutionAttempt `json:"attempts"`
	AttachmentComplete bool                `json:"attachment_complete"`
	WikiStatus         string              `json:"wiki_status"`
	Gaps               []string            `json:"gaps"`
	Readiness          string              `json:"readiness"`
}

// ContextResult is supporting state, not a final investigation report. An
// unresolved context remains input to local remediation and the future planner.
type ContextResult struct {
	Intake  contract.Ref
	Wiki    contract.Ref
	Context contract.Ref
	Ready   bool
}
