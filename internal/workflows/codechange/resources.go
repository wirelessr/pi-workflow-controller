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

var schemaNames = []string{"input", "implement", "review"}

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
