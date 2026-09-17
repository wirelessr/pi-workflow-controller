package triage

import (
	"embed"
	"pi-workflow-controller/internal/contract"
)

//go:embed schemas/*.json
var resources embed.FS

const schemaURI = "https://pi-workflow-controller.local/schemas/triage/"

func Resources() []contract.Resource {
	var out []contract.Resource
	for _, name := range []string{"common", "intake", "wiki", "context", "planner"} {
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
	return []contract.SchemaDefinition{
		{ID: IntakeSchema, URI: schemaURI + "intake.v1.json"},
		{ID: WikiSchema, URI: schemaURI + "wiki.v1.json"},
		{ID: ContextSchema, URI: schemaURI + "context.v1.json"},
		{ID: PlannerSchema, URI: schemaURI + "planner.v1.json"},
	}
}
