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

var schemaNames = []string{"input"}

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
