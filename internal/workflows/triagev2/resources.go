package triagev2

import (
	"embed"

	"pi-workflow-controller/internal/contract"
)

//go:embed schemas/*.json
var resources embed.FS

// schemaURI is the final location; renaming the package must not change it.
const schemaURI = "https://pi-workflow-controller.local/schemas/triage/"

func Resources() []contract.Resource {
	var out []contract.Resource
	for _, name := range []string{"common", "skills"} {
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
	return []contract.SchemaDefinition{{ID: SkillsSchema, URI: schemaURI + "skills.v1.json"}}
}
