package triage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"pi-workflow-controller/internal/contract"
)

// AcquisitionConfigLimit bounds stdin before signal handling is installed.
const AcquisitionConfigLimit = 64 << 10

// RunAcquisition is the shell helper boundary, not a Controller HTTP adapter.
// Configuration (including optional Authorization) arrives on stdin, never argv.
// It writes only an attempt-local candidate; engine commit and business acceptance
// remain separate. Failed acquisitions retain raw diagnostics; failed candidate
// writes receive best-effort cleanup, with cleanup errors returned to the caller.
func RunAcquisition(ctx context.Context, requestPath string, config []byte) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	var options acquisitionOptions
	if len(config) > AcquisitionConfigLimit {
		return errors.New("acquisition configuration exceeds size limit")
	}
	if err := json.Unmarshal(config, &options); err != nil {
		// Decoder errors can contain input, including credentials.
		return errors.New("invalid acquisition configuration")
	}
	if !filepath.IsAbs(requestPath) || filepath.Base(requestPath) != "request.json" {
		return errors.New("absolute attempt request.json path required")
	}
	root, err := os.OpenRoot(filepath.Dir(requestPath))
	if err != nil {
		return err
	}
	defer root.Close()
	f, err := root.OpenFile("request.json", os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	info, statErr := f.Stat()
	if statErr != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return errors.Join(errors.New("invalid acquisition request file"), statErr, f.Close())
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return err
	}
	var req contract.Request
	if len(raw) > 1<<20 || json.Unmarshal(raw, &req) != nil {
		return errors.New("invalid acquisition request")
	}
	var task stageTask
	if json.Unmarshal([]byte(req.Prompt), &task) != nil || task.Stage != "intake" || req.Output.SchemaID != IntakeSchema || len(req.Inputs) != 0 || req.Feedback != nil || !acquisitionKey.MatchString(task.Scope.Ticket) {
		return errors.New("initial intake request required")
	}
	if !nonblank(req.Identity.RunID) || !nonblank(req.Identity.InvocationID) || !nonblank(req.Identity.AttemptID) || !nonblank(req.Identity.DispatchToken) {
		return errors.New("acquisition request identity required")
	}
	if _, err := root.Lstat("candidate.json"); !errors.Is(err, os.ErrNotExist) {
		return errors.New("candidate path already exists or is inaccessible")
	}
	v, files, err := acquireIntake(ctx, filepath.Dir(requestPath), task.Scope, options)
	if err != nil {
		return err
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	meta := struct {
		contract.Identity
		Version  int    `json:"version"`
		SchemaID string `json:"schema_id"`
	}{req.Identity, 1, req.Output.SchemaID}
	raw, err = json.Marshal(struct {
		Meta  any    `json:"meta"`
		Data  Intake `json:"data"`
		Files []file `json:"files"`
	}{meta, v, files})
	if err != nil {
		return err
	}
	candidate, err := root.OpenFile("candidate.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = candidate.Write(raw)
	err = errors.Join(err, candidate.Close())
	if err != nil {
		return errors.Join(err, root.Remove("candidate.json"))
	}
	return nil
}
