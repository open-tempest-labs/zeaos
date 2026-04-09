package main

import "fmt"

func execIceberg(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("iceberg: subcommand required\n" +
			"  iceberg verify [<table>...]  verify snapshot SHA-256 hashes\n" +
			"                               table name or zea:// path")
	}
	switch args[0] {
	case "verify":
		return execVerify(args[1:], s)
	default:
		return fmt.Errorf("iceberg: unknown subcommand %q — valid: verify", args[0])
	}
}
