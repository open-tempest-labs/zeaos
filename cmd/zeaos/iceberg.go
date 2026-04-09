package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func execIceberg(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("iceberg: subcommand required\n" +
			"  iceberg verify [<table>...]  verify snapshot SHA-256 hashes\n" +
			"  iceberg repair <table>...    re-copy metadata to remote after a failed push")
	}
	switch args[0] {
	case "verify":
		return execVerify(args[1:], s)
	case "repair":
		return execIcebergRepair(args[1:], s)
	default:
		return fmt.Errorf("iceberg: unknown subcommand %q — valid: verify, repair", args[0])
	}
}

func execIcebergRepair(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("iceberg repair: table name required")
	}
	for _, name := range args {
		if err := repairOne(name, s); err != nil {
			fmt.Printf("  error repairing %s: %v\n", name, err)
		}
	}
	return nil
}

func repairOne(name string, s *Session) error {
	entry, err := s.Get(name)
	if err != nil {
		return err
	}
	rec, schema := resolveIcebergRecord(entry)
	if rec == nil {
		return fmt.Errorf("%q has no Iceberg push records — push with --iceberg first", name)
	}

	stagingDir := filepath.Join(s.Dir, "iceberg-staging", schema, entry.Name)
	if _, err := os.Stat(stagingDir); err != nil {
		return fmt.Errorf("no staging data found for %q — re-push to repair", name)
	}

	tableZeaPath := rec.Target + "/" + schema + "/" + rec.TableName

	fmt.Printf("Repairing %s → %s...\n", name, tableZeaPath)
	if err := s.Drive.CopyDirToMount(stagingDir, tableZeaPath); err != nil {
		return fmt.Errorf("copy metadata to remote: %w", err)
	}
	fmt.Printf("  ✓ metadata repaired\n")
	return nil
}
