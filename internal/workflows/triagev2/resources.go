package triagev2

import (
	"embed"

	"pi-workflow-controller/internal/contract"
)

//go:embed schemas/*.json
var resources embed.FS

// schemaURI is the final location; renaming the package must not change it.
const schemaURI = "https://pi-workflow-controller.local/schemas/triage/"

// schemaNames lists every resource; all but common are contract schemas
// with ID triage.<name>.v1.
var schemaNames = []string{"common", "skills", "prompt", "intake", "facts", "factcheck", "factstatus", "round"}

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
	for _, name := range schemaNames[1:] {
		out = append(out, contract.SchemaDefinition{ID: "triage." + name + ".v1", URI: schemaURI + name + ".v1.json"})
	}
	return out
}
