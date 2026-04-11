package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/open-tempest-labs/zeaberg-go"
)

// verifyCacheEntry records the result of a previous successful verify for one snapshot.
type verifyCacheEntry struct {
	SnapshotID int64     `json:"snapshot_id"`
	VerifiedAt time.Time `json:"verified_at"`
	Hash       string    `json:"hash"`
	RemotePath string    `json:"remote_path"`
}

func execVerify(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("iceberg verify: table name or zea:// path required")
	}
	for _, arg := range args {
		if err := verifyOne(arg, s); err != nil {
			fmt.Printf("  error verifying %s: %v\n", arg, err)
		}
	}
	return nil
}

func verifyOne(nameOrPath string, s *Session) error {
	var metadataPath string
	var displayName string

	if strings.HasPrefix(nameOrPath, "zea://") {
		metadataPath = s.Drive.ExpandPath(nameOrPath)
		displayName = nameOrPath
	} else {
		entry, err := s.Get(nameOrPath)
		if err != nil {
			return err
		}
		rec, schema := resolveIcebergRecord(entry)
		if rec == nil {
			return fmt.Errorf("%q has no Iceberg push records — push with --iceberg first", nameOrPath)
		}
		tableZeaPath := rec.Target + "/" + schema + "/" + rec.TableName
		metadataPath = s.Drive.ExpandPath(tableZeaPath)
		displayName = nameOrPath + " → " + tableZeaPath
	}

	// SDK mode: zeaberg reads metadata via os.ReadFile / filepath.Join which
	// cannot handle s3:// URIs. Stage the metadata files locally first.
	verifyPath := metadataPath
	var tmpDir string
	if s.Drive.IsS3Path(metadataPath) {
		var err error
		tmpDir, err = stageIcebergMetadata(metadataPath, s.Drive)
		if err != nil {
			return fmt.Errorf("stage metadata from S3: %w", err)
		}
		defer os.RemoveAll(tmpDir)
		verifyPath = tmpDir
	}

	opts := zeaberg.VerifyOptions{
		DataFileResolver: func(p string) string {
			// Resolve zea:// paths via ExpandPath (may produce s3:// in SDK mode).
			if strings.HasPrefix(p, "zea://") {
				p = s.Drive.ExpandPath(p)
			}
			if s.Drive.IsS3Path(p) {
				// Download to a temp file so zeaberg can hash it locally.
				tmp := downloadS3ToTemp(p, s.Drive)
				if tmp == "" {
					return p // let zeaberg report missing
				}
				return tmp
			}
			return p
		},
	}

	tableUUID, results, err := zeaberg.VerifyTable(verifyPath, opts)
	if err != nil {
		return fmt.Errorf("read table at %s: %w", metadataPath, err)
	}
	if len(results) == 0 {
		fmt.Printf("%s: no snapshots\n", displayName)
		return nil
	}

	fmt.Printf("%s\n", displayName)
	fmt.Printf("  %-20s  %-10s  %-12s  %-14s  %s\n", "Snapshot", "Status", "Rows", "Change", "Session")
	fmt.Println("  " + strings.Repeat("─", 82))

	verified, mismatched, unattested := 0, 0, 0
	firstVerifies, changed := 0, 0

	for _, r := range results {
		icon := statusIcon(r.Status)
		session := r.SessionID
		if len(session) > 28 {
			parts := strings.Split(session, "/")
			session = "..." + parts[len(parts)-1]
		}

		changeLabel := ""
		if r.Status == zeaberg.VerifyStatusOK {
			cached, _ := loadVerifyCache(s.Dir, tableUUID, r.SnapshotID)
			switch {
			case cached == nil:
				changeLabel = "first verify"
				firstVerifies++
			case cached.Hash == r.Actual:
				changeLabel = "unchanged"
			default:
				changeLabel = "changed"
				changed++
			}
			// Update the cache entry with this verify run.
			_ = saveVerifyCache(s.Dir, tableUUID, verifyCacheEntry{
				SnapshotID: r.SnapshotID,
				VerifiedAt: time.Now().UTC(),
				Hash:       r.Actual,
				RemotePath: metadataPath,
			})
		}

		fmt.Printf("  %-20d  %s %-10s  %-12s  %-14s  %s\n",
			r.SnapshotID, icon, r.Status, r.RowCount, changeLabel, session)
		if r.Status == zeaberg.VerifyStatusMismatch {
			fmt.Printf("    expected: %s\n", r.Expected)
			fmt.Printf("    actual:   %s\n", r.Actual)
		}

		switch r.Status {
		case zeaberg.VerifyStatusOK:
			verified++
		case zeaberg.VerifyStatusMismatch:
			mismatched++
		case zeaberg.VerifyStatusUnattested:
			unattested++
		}
	}

	fmt.Println()
	if mismatched > 0 {
		fmt.Printf("  ✗ %d snapshot(s) failed hash verification — possible tampering\n", mismatched)
	} else {
		fmt.Printf("  ✓ %d snapshot(s) verified", verified)
		if firstVerifies > 0 {
			fmt.Printf(", %d new baseline(s) established", firstVerifies)
		}
		if changed > 0 {
			fmt.Printf(", %d changed since last verify", changed)
		}
		if unattested > 0 {
			fmt.Printf(", %d unattested (external writes)", unattested)
		}
		fmt.Println()
	}
	return nil
}

// verifyCachePath returns the path for a per-snapshot verify cache file.
func verifyCachePath(sessionDir, tableUUID string, snapshotID int64) string {
	return filepath.Join(sessionDir, "verify-cache", tableUUID, fmt.Sprintf("%d.json", snapshotID))
}

func loadVerifyCache(sessionDir, tableUUID string, snapshotID int64) (*verifyCacheEntry, error) {
	data, err := os.ReadFile(verifyCachePath(sessionDir, tableUUID, snapshotID))
	if err != nil {
		return nil, err
	}
	var e verifyCacheEntry
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

func saveVerifyCache(sessionDir, tableUUID string, e verifyCacheEntry) error {
	p := verifyCachePath(sessionDir, tableUUID, e.SnapshotID)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0644)
}

// stageIcebergMetadata downloads the version-hint and metadata JSON from an
// s3:// table location into a temp directory that zeaberg can read locally.
func stageIcebergMetadata(s3TablePath string, drive *DriveManager) (string, error) {
	tmpDir, err := os.MkdirTemp("", "zeaos-verify-*")
	if err != nil {
		return "", err
	}
	metaDir := filepath.Join(tmpDir, "metadata")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		os.RemoveAll(tmpDir)
		return "", err
	}

	hintURI := s3TablePath + "/metadata/version-hint.text"
	hintData, err := drive.ReadS3Path(hintURI)
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("read version-hint.text: %w", err)
	}
	if err := os.WriteFile(filepath.Join(metaDir, "version-hint.text"), hintData, 0644); err != nil {
		os.RemoveAll(tmpDir)
		return "", err
	}

	// Determine the metadata file version from the hint.
	versionStr := strings.TrimSpace(string(hintData))
	metaURI := s3TablePath + "/metadata/v" + versionStr + ".metadata.json"
	metaData, err := drive.ReadS3Path(metaURI)
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("read metadata json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(metaDir, "v"+versionStr+".metadata.json"), metaData, 0644); err != nil {
		os.RemoveAll(tmpDir)
		return "", err
	}
	return tmpDir, nil
}

// downloadS3ToTemp downloads an S3 object to a temp file and returns its path.
// The caller is responsible for deleting it. Returns "" on error.
func downloadS3ToTemp(s3URI string, drive *DriveManager) string {
	data, err := drive.ReadS3Path(s3URI)
	if err != nil {
		return ""
	}
	tmp, err := os.CreateTemp("", "zeaos-verify-data-*")
	if err != nil {
		return ""
	}
	defer tmp.Close()
	if _, err := tmp.Write(data); err != nil {
		os.Remove(tmp.Name())
		return ""
	}
	return tmp.Name()
}

// resolveIcebergRecord returns the most recent Iceberg push record and its
// schema for the given entry, or nil if none exists.
func resolveIcebergRecord(entry *TableEntry) (*PushRecord, string) {
	for i := len(entry.PushRecords) - 1; i >= 0; i-- {
		rec := entry.PushRecords[i]
		if rec.Format == "iceberg" {
			return &entry.PushRecords[i], rec.Schema
		}
	}
	return nil, ""
}

func statusIcon(s zeaberg.VerifyStatus) string {
	switch s {
	case zeaberg.VerifyStatusOK:
		return "✓"
	case zeaberg.VerifyStatusMismatch:
		return "✗"
	case zeaberg.VerifyStatusUnattested:
		return "○"
	case zeaberg.VerifyStatusMissing:
		return "?"
	}
	return " "
}
