package triagev2

import (
	"embed"
	"strings"

	"pi-workflow-controller/internal/contract"
)

//go:embed schemas/*.json
var resources embed.FS

// schemaURI is the final location; renaming the package must not change it.
const schemaURI = "https://pi-workflow-controller.local/schemas/triage/"

func Resources() []contract.Resource {
	var out []contract.Resource
	for _, name := range []string{"common", "skills", "prompt", "intake", "facts", "factcheck", "factstatus"} {
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
	for _, id := range []string{SkillsSchema, PromptSchema, IntakeSchema, FactsSchema, FactCheckSchema, FactStatusSchema} {
		name := strings.TrimSuffix(strings.TrimPrefix(id, "triage."), ".v1")
		out = append(out, contract.SchemaDefinition{ID: id, URI: schemaURI + name + ".v1.json"})
	}
	return out
}
