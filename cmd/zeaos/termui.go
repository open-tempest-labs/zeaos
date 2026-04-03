package main

import (
	"os"

	"golang.org/x/term"
)

// termWidth returns the current terminal width, defaulting to 80.
func termWidth() int {
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 {
		return 80
	}
	return w
}
