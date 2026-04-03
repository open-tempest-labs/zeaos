package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/spf13/cobra"
)

// version is set at build time via -ldflags "-X main.version=x.y.z"
var version = "dev"

func main() {
	root := &cobra.Command{
		Use:           "zeaos",
		Short:         "ZeaOS — Zero-copy Data REPL from Open Tempest Labs",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE:          runREPL,
	}
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "zeaos: %v\n", err)
		os.Exit(1)
	}
}

func runREPL(cmd *cobra.Command, args []string) error {
	session, err := NewSession()
	if err != nil {
		return fmt.Errorf("session init: %w", err)
	}
	defer session.Close()

	printStartup(session)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "ZeaOS> ",
		HistoryFile:     filepath.Join(session.Dir, "history"),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return fmt.Errorf("readline: %w", err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			}
			continue
		}
		if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}

		if execErr := execLine(line, session); execErr != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", execErr)
		}
	}

	fmt.Println("\nGoodbye.")
	return nil
}
