package zeaberg

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"
)

// VerifyStatus describes the verification outcome for a single snapshot.
type VerifyStatus int

const (
	// VerifyStatusOK means the data file hash matches the recorded hash.
	VerifyStatusOK VerifyStatus = iota
	// VerifyStatusMismatch means the data file hash does not match — possible tampering.
	VerifyStatusMismatch
	// VerifyStatusUnattested means the snapshot has no zea.data_sha256 — written by an external tool.
	VerifyStatusUnattested
	// VerifyStatusMissing means the data file referenced in the snapshot cannot be found.
	VerifyStatusMissing
)

func (s VerifyStatus) String() string {
	switch s {
	case VerifyStatusOK:
		return "verified"
	case VerifyStatusMismatch:
		return "mismatch"
	case VerifyStatusUnattested:
		return "unattested"
	case VerifyStatusMissing:
		return "missing"
	}
	return "unknown"
}

// SnapshotVerification holds the verification result for one snapshot.
type SnapshotVerification struct {
	SnapshotID int64
	Timestamp  time.Time
	Status     VerifyStatus
	DataFile   string // canonical path registered in snapshot summary
	Expected   string // zea.data_sha256 from metadata
	Actual     string // recomputed from data file; empty if unattested or missing
	SessionID  string
	RowCount   string // "added-records" from snapshot summary
}

// VerifyOptions configures a VerifyTable call.
type VerifyOptions struct {
	// DataFileResolver maps a canonical data file path (as stored in
	// zea.data_file) to a locally readable path. Use this when the canonical
	// path is on a remote or FUSE mount that cannot be read directly.
	// If nil, the canonical path is used as-is.
	DataFileResolver func(canonicalPath string) string
}

// VerifyTable reads the Iceberg table at location and verifies each snapshot
// that carries a zea.data_sha256 property. Snapshots written by external tools
// (no zea.data_sha256) are reported as VerifyStatusUnattested, not as failures.
func VerifyTable(location string, opts ...VerifyOptions) ([]SnapshotVerification, error) {
	var resolver func(string) string
	if len(opts) > 0 && opts[0].DataFileResolver != nil {
		resolver = opts[0].DataFileResolver
	}

	meta, _, err := readMetadata(location)
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}

	results := make([]SnapshotVerification, 0, len(meta.Snapshots))
	for _, snap := range meta.Snapshots {
		result := SnapshotVerification{
			SnapshotID: snap.SnapshotID,
			Timestamp:  time.UnixMilli(snap.TimestampMs),
			SessionID:  snap.Summary["zea.session_id"],
			RowCount:   snap.Summary["added-records"],
			DataFile:   snap.Summary["zea.data_file"],
			Expected:   snap.Summary["zea.data_sha256"],
		}

		if result.Expected == "" {
			result.Status = VerifyStatusUnattested
			results = append(results, result)
			continue
		}

		if result.DataFile == "" {
			result.Status = VerifyStatusMissing
			results = append(results, result)
			continue
		}

		// Resolve to a locally readable path if a resolver is provided.
		readPath := result.DataFile
		if resolver != nil {
			readPath = resolver(result.DataFile)
		}

		actual, err := hashFile(readPath)
		if err != nil {
			result.Status = VerifyStatusMissing
			results = append(results, result)
			continue
		}

		result.Actual = actual
		if actual == result.Expected {
			result.Status = VerifyStatusOK
		} else {
			result.Status = VerifyStatusMismatch
		}
		results = append(results, result)
	}
	return results, nil
}

// hashFile computes the SHA-256 hex digest of a file's contents.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
