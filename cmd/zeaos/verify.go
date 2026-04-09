package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/open-tempest-labs/zeaberg-go"
)

func execVerify(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("verify: table name or zea:// path required")
	}
	for _, arg := range args {
		if err := verifyOne(arg, s); err != nil {
			fmt.Printf("  error verifying %s: %v\n", arg, err)
		}
	}
	return nil
}

func verifyOne(nameOrPath string, s *Session) error {
	var metadataPath string  // local path for reading Iceberg metadata
	var spillFile string     // local Parquet spill file for data hashing

	if strings.HasPrefix(nameOrPath, "zea://") {
		// Direct path — use FUSE mount. No spill file available.
		metadataPath = s.Drive.ExpandPath(nameOrPath)
	} else {
		entry, err := s.Get(nameOrPath)
		if err != nil {
			return err
		}
		// Use the local staging directory for metadata — it's identical to
		// what was copied to FUSE and avoids FUSE small-file I/O issues.
		rec, schema := resolveIcebergRecord(entry)
		if rec == nil {
			return fmt.Errorf("%q has no Iceberg push records — push with --iceberg first", nameOrPath)
		}
		metadataPath = filepath.Join(s.Dir, "iceberg-staging", schema, entry.Name)
		spillFile = entry.FilePath
	}

	opts := zeaberg.VerifyOptions{}
	if spillFile != "" {
		// Map any canonical data file path back to the local spill file.
		// The bytes are identical — the hash is content-based, not path-based.
		localSpill := spillFile
		opts.DataFileResolver = func(_ string) string { return localSpill }
	}

	results, err := zeaberg.VerifyTable(metadataPath, opts)
	if err != nil {
		return fmt.Errorf("read table at %s: %w", metadataPath, err)
	}
	if len(results) == 0 {
		fmt.Printf("%s: no snapshots\n", nameOrPath)
		return nil
	}

	fmt.Printf("%s\n", nameOrPath)
	fmt.Printf("  %-20s  %-10s  %-12s  %s\n", "Snapshot", "Status", "Rows", "Session")
	fmt.Println("  " + strings.Repeat("─", 70))

	verified, mismatched, unattested := 0, 0, 0
	for _, r := range results {
		icon := statusIcon(r.Status)
		session := r.SessionID
		if len(session) > 30 {
			parts := strings.Split(session, "/")
			session = "..." + parts[len(parts)-1]
		}
		fmt.Printf("  %-20d  %s %-10s  %-12s  %s\n",
			r.SnapshotID, icon, r.Status, r.RowCount, session)
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
		fmt.Printf("  ✓ %d ZeaOS snapshot(s) verified", verified)
		if unattested > 0 {
			fmt.Printf(", %d unattested (external writes)", unattested)
		}
		fmt.Println()
	}
	return nil
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
