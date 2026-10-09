package triage

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// TimeAnchor is one observed incident timestamp. OffsetSeconds is the
// calculated offset, not an assumed geographical zone; a local timestamp is
// normalized only through a same-event absolute timestamp.
type TimeAnchor struct {
	ID                string    `json:"id"`
	Event             string    `json:"event"`
	Original          string    `json:"original"`
	Format            string    `json:"format"`
	SourceTZ          string    `json:"source_tz"`
	UTC               string    `json:"utc"`
	OffsetSeconds     int       `json:"offset_seconds"`
	Evidence          Evidence  `json:"evidence"`
	PairedEpochMillis *int64    `json:"paired_epoch_millis"`
	PairedEvidence    *Evidence `json:"paired_evidence"`
}

// checkAnchor recomputes the UTC conversion from the reported structured
// values. It does not read the source text; whether original matches the
// source is the validator's judgment.
func checkAnchor(field string, a TimeAnchor, evidence func(string, Evidence) error) error {
	if err := evidence(field+".evidence", a.Evidence); err != nil {
		return err
	}
	want, err := utc(a.UTC)
	if err != nil {
		return fmt.Errorf("%s.utc: %w", field, err)
	}
	var actual time.Time
	switch a.Format {
	case "rfc3339":
		actual, err = time.Parse(time.RFC3339Nano, a.Original)
		if _, offset := actual.Zone(); err == nil && offset != a.OffsetSeconds {
			return fmt.Errorf("%s.offset_seconds: got %d; original %q carries offset %d", field, a.OffsetSeconds, a.Original, offset)
		}
	case "epoch-seconds", "epoch-millis":
		n, e := strconv.ParseInt(a.Original, 10, 64)
		err = e
		if a.Format == "epoch-seconds" {
			actual = time.Unix(n, 0)
		} else {
			actual = time.UnixMilli(n)
		}
		if a.OffsetSeconds != 0 || a.SourceTZ != "UTC" {
			return fmt.Errorf("%s: got offset_seconds %d and source_tz %q; an epoch value is UTC, want 0 and UTC", field, a.OffsetSeconds, a.SourceTZ)
		}
	case "local-paired":
		if a.PairedEpochMillis == nil || a.PairedEvidence == nil {
			return fmt.Errorf("%s: format local-paired needs paired_epoch_millis and paired_evidence from the same event; an absolute timestamp in another syntax is rfc3339 (normalize original, keep the raw form in event or evidence)", field)
		}
		// The pairing and the arithmetic are independent: report both so one
		// repair can fix them together.
		var pairErr error
		if reflect.DeepEqual(a.Evidence, *a.PairedEvidence) {
			pairErr = fmt.Errorf("%s.paired_evidence: got the same file_id %q and locator as evidence, so the local timestamp would be its own absolute evidence; want the locator of the same event's absolute timestamp (a separate span, which may be in the same file)", field, a.PairedEvidence.FileID)
		} else if err := evidence(field+".paired_evidence", *a.PairedEvidence); err != nil {
			return err
		}
		local, e := time.Parse("2006-01-02T15:04:05.999999999", a.Original)
		err = e
		actual = time.UnixMilli(*a.PairedEpochMillis)
		var mathErr error
		if shifted := local.Add(-time.Duration(a.OffsetSeconds) * time.Second); e == nil && !shifted.Equal(actual) {
			got := fmt.Sprintf("local %q minus %d seconds is %s, but paired_epoch_millis %d is %s", a.Original, a.OffsetSeconds, shifted.Format(time.RFC3339Nano), *a.PairedEpochMillis, actual.UTC().Format(time.RFC3339Nano))
			diff := shifted.Sub(actual)
			// The schema bounds offset_seconds; past it no offset can close the gap.
			switch want := a.OffsetSeconds + int(diff/time.Second); {
			case diff%time.Second == 0 && want >= -86400 && want <= 86400:
				mathErr = fmt.Errorf("%s.offset_seconds: %s; want offset_seconds %d", field, got, want)
			case diff > -time.Second && diff < time.Second:
				mathErr = fmt.Errorf("%s: %s; they differ by %s, below one second, which no whole-second offset_seconds can close, so original and paired_epoch_millis disagree", field, got, diff)
			default:
				mathErr = fmt.Errorf("%s: %s; they differ by %s, which no offset_seconds within ±86400 seconds can close, so original and paired_epoch_millis disagree", field, got, diff)
			}
		}
		if pairErr != nil || mathErr != nil {
			return errors.Join(pairErr, mathErr)
		}
	default:
		return fmt.Errorf("%s.format: unsupported %q", field, a.Format)
	}
	if a.Format != "local-paired" && (a.PairedEpochMillis != nil || a.PairedEvidence != nil) {
		return fmt.Errorf("%s: paired_epoch_millis and paired_evidence must be null unless format is local-paired", field)
	}
	if err != nil {
		return fmt.Errorf("%s.original: %q does not parse as format %s", field, a.Original, a.Format)
	}
	if !actual.Equal(want) {
		return fmt.Errorf("%s.utc: original %q (format %s, offset_seconds %d) converts to %s but utc is %q", field, a.Original, a.Format, a.OffsetSeconds, actual.UTC().Format(time.RFC3339Nano), a.UTC)
	}
	return nil
}
