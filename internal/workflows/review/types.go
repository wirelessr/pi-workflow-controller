package review

import "pi-workflow-controller/internal/contract"

const (
	PrepareSchema    = "review.prepare.v1"
	ReviewerSchema   = "review.reviewer.v1"
	ValidationSchema = "review.validation.v1"
)

type Pin struct {
	URL        string `json:"url"`
	Repository string `json:"repository"`
	Number     int    `json:"number"`
	BaseSHA    string `json:"base_sha"`
	HeadSHA    string `json:"head_sha"`
	MergeBase  string `json:"merge_base"`
	DiffRange  string `json:"diff_range"`
	ContextID  string `json:"context_id"`
}

type Source struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	URL    string `json:"url"`
	Status string `json:"status"`
	FileID string `json:"file_id"`
	Note   string `json:"note"`
}

type Requirement struct {
	ID        string   `json:"id"`
	Kind      string   `json:"kind"`
	Statement string   `json:"statement"`
	SourceIDs []string `json:"source_ids"`
}

type Prepared struct {
	Pin           Pin           `json:"pin"`
	Sources       []Source      `json:"sources"`
	Requirements  []Requirement `json:"requirements"`
	OpenQuestions []string      `json:"open_questions"`
	ContextFile   string        `json:"context_file"`
}

type Evidence struct {
	Path    string `json:"path"`
	Line    int    `json:"line"`
	EndLine int    `json:"end_line"`
	Detail  string `json:"detail"`
}

type Finding struct {
	ID       string     `json:"id"`
	Severity string     `json:"severity"`
	Title    string     `json:"title"`
	Location Evidence   `json:"location"`
	Evidence []Evidence `json:"evidence"`
	Impact   string     `json:"impact"`
}

type Assessment struct {
	RequirementID string     `json:"requirement_id"`
	Status        string     `json:"status"`
	Evidence      []Evidence `json:"evidence"`
	Reason        string     `json:"reason"`
}

type Reviewed struct {
	Pin          Pin          `json:"pin"`
	Role         string       `json:"role"`
	Context      contract.Ref `json:"context"`
	Coverage     []string     `json:"coverage"`
	Limitations  []string     `json:"limitations"`
	Findings     []Finding    `json:"findings"`
	Requirements []Assessment `json:"requirements"`
}

type ReviewerResult struct {
	Role   string        `json:"role"`
	Status string        `json:"status"`
	Ref    *contract.Ref `json:"ref"`
	Detail string        `json:"detail"`
}

type Disposition struct {
	FindingID string `json:"finding_id"`
	Action    string `json:"action"`
	TargetID  string `json:"target_id"`
	Reason    string `json:"reason"`
}

type Validated struct {
	Pin          Pin              `json:"pin"`
	Context      contract.Ref     `json:"context"`
	Reviewers    []ReviewerResult `json:"reviewers"`
	Findings     []Finding        `json:"findings"`
	Dispositions []Disposition    `json:"dispositions"`
	Requirements []Assessment     `json:"requirements"`
	Completeness string           `json:"completeness"`
	Conclusion   string           `json:"conclusion"`
	Limitations  []string         `json:"limitations"`
	ReportFile   string           `json:"report_file"`
}
