package codechange

import (
	"embed"

	"pi-workflow-controller/internal/contract"
)

//go:embed schemas/*.json
var resources embed.FS

const schemaURI = "https://pi-workflow-controller.local/schemas/code-change/"

// InputSchema is the Controller record of the parsed prompt, the repository
// and the worktree; the design doc bytes are its evidence file design-doc.
const InputSchema = "code-change.input.v1"

// ImplementSchema is the implementer's readiness and local implementation.
const ImplementSchema = "code-change.implement.v1"

// ReviewSchema is a pre-PR reviewer's findings; both reviewers use it.
const ReviewSchema = "code-change.review.v1"

// PublishSchema is the correction Step's dispositions and publication receipt.
const PublishSchema = "code-change.publish.v1"

// PullRequestSchema is the Controller record of the verified pull request.
const PullRequestSchema = "code-change.pull-request.v1"

// FixSchema is the fix Step's dispositions of the deep review report's
// findings and its push receipt.
const FixSchema = "code-change.fix.v1"

// ExternalObjectsSchema is the Controller record of what exists after a
// failure from the dispatch of a Step that writes to GitHub through its
// closing.
const ExternalObjectsSchema = "code-change.external-objects.v1"

var schemaNames = []string{"input", "implement", "review", "publish", "pull-request", "fix", "external-objects"}

func Resources() []contract.Resource {
	var out []contract.Resource
	for _, name := range schemaNames {
		path := name + ".v1.json"
		raw, err := resources.ReadFile("schemas/" + path)
		if err != nil {
			panic(err)
		}
		out = append(out, contract.Resource{URI: schemaURI + path, JSON: raw})
	}
	return out
}

func Schemas() []contract.SchemaDefinition {
	var out []contract.SchemaDefinition
	for _, name := range schemaNames {
		out = append(out, contract.SchemaDefinition{ID: "code-change." + name + ".v1", URI: schemaURI + name + ".v1.json"})
	}
	return out
}
