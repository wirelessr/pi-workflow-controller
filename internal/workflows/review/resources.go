package review

import (
	"embed"
	"errors"
	"io/fs"
	"path/filepath"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/reportresource"
)

//go:embed skills schemas
var reviewResources embed.FS

const reviewSchemaURI = "https://pi-workflow-controller.local/schemas/review/"

func Resources() []contract.Resource {
	entries, err := reviewResources.ReadDir("schemas")
	if err != nil {
		panic(err)
	}
	resources := make([]contract.Resource, 0, len(entries))
	for _, entry := range entries {
		data, err := reviewResources.ReadFile("schemas/" + entry.Name())
		if err != nil {
			panic(err)
		}
		resources = append(resources, contract.Resource{URI: reviewSchemaURI + entry.Name(), JSON: data})
	}
	return resources
}

func Schemas() []contract.SchemaDefinition {
	return []contract.SchemaDefinition{
		{ID: PrepareSchema, URI: reviewSchemaURI + "prepare.v1.json"},
		{ID: ReviewerSchema, URI: reviewSchemaURI + "reviewer.v1.json"},
		{ID: ValidationSchema, URI: reviewSchemaURI + "validation.v1.json"},
	}
}

// ExtractSkills reserves a fresh directory in an existing run. Failed partial
// extractions remain reserved rather than risking deletion of replaced content.
func ExtractSkills(runDir string) (map[string]string, error) {
	if runDir == "" {
		return nil, errors.New("review skill run directory is empty")
	}
	skills, err := fs.Sub(reviewResources, "skills")
	if err != nil {
		return nil, err
	}
	root, err := reportresource.ExtractFresh(runDir, "review-skills", skills, reportresource.Common)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]string, 5)
	for _, role := range []string{"prepare", "code", "scale", "simplicity", "validate"} {
		paths[role] = filepath.Join(root, "pwc-review-"+role, "SKILL.md")
	}
	return paths, nil
}
