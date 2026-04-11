package main

import (
	"fmt"
	"strings"
)

// execModel dispatches model subcommands.
// model promote / unpromote / list / validate / export / push / publish
func execModel(args []string, s *Session) error {
	if len(args) == 0 {
		return execModelHelp()
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "promote":
		return execPromote(rest, s)
	case "unpromote":
		return execUnpromote(rest, s)
	case "list":
		return execListPromotions(s)
	case "validate":
		return execValidate(rest, s)
	case "export":
		return execExport(rest, s)
	case "push":
		return execModelPush(rest, s)
	case "publish":
		return execPublish(rest, s)
	default:
		return fmt.Errorf("model: unknown subcommand %q\nTry: promote, unpromote, list, validate, export, push, publish", sub)
	}
}

// execModelPush pushes the source (load-node) tables for all promoted models.
// Unlike bare 'push', which pushes all session tables, 'model push' scopes the
// push to exactly the data that the promoted models depend on.
func execModelPush(args []string, s *Session) error {
	if len(s.Promoted) == 0 {
		return fmt.Errorf("model push: no promotions in session — use 'model promote <table>' first")
	}

	pa, err := parsePushArgs(args)
	if err != nil {
		return err
	}

	// Collect unique source (load-node) tables from all promoted artifacts' lineage.
	seen := map[string]bool{}
	for _, art := range s.Promoted {
		chain, err := walkLineage(s, art.PromotedFrom)
		if err != nil {
			continue
		}
		for _, node := range chain.Nodes {
			if node.NodeKind == "load" && !seen[node.Entry.Name] {
				seen[node.Entry.Name] = true
				pa.Tables = append(pa.Tables, node.Entry.Name)
			}
		}
	}

	if len(pa.Tables) == 0 {
		return fmt.Errorf("model push: could not resolve source tables from promotions — check lineage with 'hist'")
	}

	fmt.Printf("Pushing source data for %d model(s): %s\n",
		len(s.Promoted), strings.Join(pa.Tables, ", "))
	return execPushData(pa, s)
}

func execModelHelp() error {
	fmt.Print(`model — define and publish data models from session tables

  model promote <table> [as <name>] [model|semantic]
                              mark a session table as a named model artifact
                                <name>    export name (default: table name)
                                model     a SQL model for downstream tooling (default)
                                semantic  a semantic layer metric or entity
  model unpromote <name>...   remove one or more promotions
  model list                  list all promoted models
  model validate [<name>]     check SQL portability for export
  model export [-o DIR]       write model SQL + sources.yml bundle
                                (default output dir: ./zea-model-export)
  model push --target <dest>  push source data for all promoted models
                                --target md:database    push to MotherDuck
                                --target zea://...      push to ZeaDrive
                                --target zea://... --iceberg  push as Iceberg
  model publish [<name>]      publish model SQL to a Git repository

`)
	return nil
}
