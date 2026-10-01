package triage

import (
	"encoding/json"
	"regexp"
)

var jiraKey = regexp.MustCompile(`^[A-Z][A-Z0-9_]*-[1-9][0-9]*$`)

// Raw Jira issue projections used only to cross-check an Agent-produced
// intake inventory against the raw issue it saved.
type jiraIssue struct {
	Key    string                     `json:"key"`
	Fields map[string]json.RawMessage `json:"fields"`
}
type jiraAttachment struct {
	ID      string `json:"id"`
	Size    *int64 `json:"size"`
	Content string `json:"content"`
	Mime    string `json:"mimeType"`
}
type jiraLink struct {
	In *struct {
		Key string `json:"key"`
	} `json:"inwardIssue"`
	Out *struct {
		Key string `json:"key"`
	} `json:"outwardIssue"`
}
