package review

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"pi-workflow-controller/internal/contract"
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
	absolute, err := filepath.Abs(runDir)
	if err != nil {
		return nil, err
	}
	run, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, err
	}
	defer func(run *os.Root) { _ = run.Close() }(run)
	if err := run.Mkdir("review-skills", 0700); err != nil {
		return nil, fmt.Errorf("reserve review skills: %w", err)
	}
	root, err := run.OpenRoot("review-skills")
	if err != nil {
		return nil, err
	}
	defer func(root *os.Root) { _ = root.Close() }(root)
	err = fs.WalkDir(reviewResources, "skills", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == "skills" {
			return nil
		}
		rel := strings.TrimPrefix(path, "skills/")
		if entry.IsDir() {
			return root.Mkdir(rel, 0700)
		}
		data, err := reviewResources.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := root.OpenFile(rel, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(data)
		return errors.Join(writeErr, file.Close())
	})
	if err != nil {
		return nil, fmt.Errorf("extract review skills: %w", err)
	}
	paths := make(map[string]string, 5)
	for _, role := range []string{"prepare", "code", "scale", "simplicity", "validate"} {
		paths[role] = filepath.Join(absolute, "review-skills", "pwc-review-"+role, "SKILL.md")
	}
	return paths, nil
}
