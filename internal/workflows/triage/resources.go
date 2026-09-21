package triage

import (
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/reportresource"
)

//go:embed schemas/*.json report/*.py
var resources embed.FS

const schemaURI = "https://pi-workflow-controller.local/schemas/triage/"

func Resources() []contract.Resource {
	var out []contract.Resource
	for _, name := range []string{"common", "intake", "wiki", "context", "planner", "worker", "claim", "verification", "report"} {
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
		{ID: WorkerSchema, URI: schemaURI + "worker.v1.json"},
		{ID: ClaimSchema, URI: schemaURI + "claim.v1.json"},
		{ID: VerificationSchema, URI: schemaURI + "verification.v1.json"},
		{ID: ReportSchema, URI: schemaURI + "report.v1.json"},
	}
}

// ExtractReport reserves the report resources once per run, before report work.
func ExtractReport(runDir string) (string, error) {
	if runDir == "" {
		return "", fmt.Errorf("triage report run directory is empty")
	}
	tree, err := fs.Sub(resources, "report")
	if err != nil {
		return "", err
	}
	root, err := reportresource.ExtractFresh(runDir, "triage-report", tree, reportresource.Common)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "render_report.py"), nil
}
