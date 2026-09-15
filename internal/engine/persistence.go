package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"pi-workflow-controller/internal/contract"
)

// All journal and snapshot operations run on the control sequence (r.mu).
func (r *Run) writeLocked(path, phase string, value any) (err error) {
	if r.beforeIO != nil {
		r.beforeIO(path, phase)
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(path), ".engine-"+contract.NewID())
	f, err := r.fs.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = r.fs.Remove(tmp) }()
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	return r.fs.Rename(tmp, path)
}
func sourceFailure(err error, source []contract.Identity) *Failure {
	f := normalize(err, "")
	if len(source) > 0 {
		f.RunID = source[0].RunID
		f.StepID = source[0].InvocationID
		f.AttemptID = source[0].AttemptID
	}
	return f
}
func (r *Run) appendLocked(kind string, details any, source []contract.Identity) (uint64, error) {
	if r.journalBroken != nil {
		if r.locked {
			f := newFailure(JournalFailed, kind, "journal is unavailable")
			f.Cause = r.journalBroken
			r.finalErrorLocked(f)
		}
		return 0, r.journalBroken
	}
	if r.beforeIO != nil {
		r.beforeIO("events.jsonl", kind)
	}
	seq := r.state.LastSeq + 1
	raw, err := json.Marshal(Event{1, seq, time.Now().UTC(), r.state.RunID, kind, details})
	if err == nil && int64(len(raw)+1) > r.definition.Policy.MaxJournalBytes-r.journalBytes {
		f := newFailure(LimitExceeded, kind, "core journal byte limit exceeded")
		f.LimitScope = "run"
		err = f
	}
	if err == nil {
		info, e := r.fs.Lstat("events.jsonl")
		if e != nil {
			err = e
		} else if !info.Mode().IsRegular() {
			err = fmt.Errorf("journal is not a regular file")
		}
	}
	if err == nil {
		var f *os.File
		f, err = r.fs.OpenFile("events.jsonl", os.O_WRONLY|os.O_APPEND, 0600)
		if err == nil {
			var n int
			n, err = f.Write(append(raw, '\n'))
			if err == nil && n != len(raw)+1 {
				err = io.ErrShortWrite
			}
			if err == nil {
				err = f.Sync()
			}
			err = errors.Join(err, f.Close())
		}
	}
	if err != nil {
		if !fatal(err) {
			f := newFailure(JournalFailed, kind, "journal append/Sync failed")
			f.Origin = OriginStorage
			f.Cause = err
			err = f
		}
		err = sourceFailure(err, source)
		r.journalBroken = err
		r.ioFailureLocked(err)
		return 0, err
	}
	r.journalBytes += int64(len(raw) + 1)
	return seq, nil
}
func (r *Run) commitLocked(kind string, details any, apply func(uint64), source ...contract.Identity) error {
	seq, err := r.appendLocked(kind, details, source)
	if err != nil {
		return err
	}
	apply(seq)
	r.state.LastSeq = seq
	if err = r.writeLocked("run.json", kind, r.state); err != nil {
		f := newFailure(StorageFailed, kind, "run snapshot write failed")
		f.Origin = OriginStorage
		f.Cause = err
		f = sourceFailure(f, source)
		r.ioFailureLocked(f)
		return f
	}
	r.notifyLocked()
	return nil
}
func (r *Run) ioFailureLocked(err error) {
	r.state.StatePersisted = false
	if r.locked {
		r.finalErrorLocked(err)
	} else {
		r.stopLocked(err)
	}
	r.notifyLocked()
}
func (r *Run) finalErrorLocked(err error) {
	f := newFailure(FinalizationFailed, normalize(err, "").Phase, "finalization persistence failed")
	f.Origin = OriginStorage
	f.Cause = err
	r.finalErrors = append(r.finalErrors, f)
	r.state.FinalizationErrors = append(r.state.FinalizationErrors, failureInfo(f))
	r.state.StatePersisted = false
}
func (r *Run) storageLocked(path, phase string, value any, source ...contract.Identity) error {
	if err := r.writeLocked(path, phase, value); err != nil {
		f := newFailure(StorageFailed, phase, "write "+path+" failed")
		f.Origin = OriginStorage
		f.Cause = err
		f = sourceFailure(f, source)
		r.ioFailureLocked(f)
		return f
	}
	return nil
}
