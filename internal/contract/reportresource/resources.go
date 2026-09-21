// Package reportresource contains the file mechanics shared by report consumers.
package reportresource

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

//go:embed pwc_report_io.py
var Common embed.FS

// ExtractFresh reserves an existing run's child directory. Partial extractions
// stay reserved: removing them could delete content replaced by another owner.
func ExtractFresh(runDir, name string, sources ...fs.FS) (string, error) {
	absolute, err := filepath.Abs(runDir)
	if err != nil {
		return "", err
	}
	run, err := os.OpenRoot(absolute)
	if err != nil {
		return "", err
	}
	defer func() { _ = run.Close() }()
	label := strings.ReplaceAll(name, "-", " ")
	if err := run.Mkdir(name, 0700); err != nil {
		return "", fmt.Errorf("reserve %s: %w", label, err)
	}
	root, err := run.OpenRoot(name)
	if err != nil {
		return "", err
	}
	defer func() { _ = root.Close() }()
	for _, source := range sources {
		err = fs.WalkDir(source, ".", func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if path == "." {
				return nil
			}
			if entry.IsDir() {
				return root.Mkdir(path, 0700)
			}
			data, err := fs.ReadFile(source, path)
			if err != nil {
				return err
			}
			file, err := root.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			_, writeErr := file.Write(data)
			return errors.Join(writeErr, file.Close())
		})
		if err != nil {
			return "", fmt.Errorf("extract %s: %w", label, err)
		}
	}
	return filepath.Join(absolute, name), nil
}
