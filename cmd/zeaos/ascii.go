package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const art = `ZZZZZZZZZZZZZZZZZZZ                                        OOOOOOOOO        SSSSSSSSSSSSSSS
Z:::::::::::::::::Z                                      OO:::::::::OO    SS:::::::::::::::S
Z:::::::::::::::::Z                                    OO:::::::::::::OO S:::::SSSSSS::::::S
Z:::ZZZZZZZZ:::::Z                                    O:::::::OOO:::::::OS:::::S     SSSSSSS
ZZZZZ     Z:::::Z      eeeeeeeeeeee    aaaaaaaaaaaaa  O::::::O   O::::::OS:::::S
        Z:::::Z      ee::::::::::::ee  a::::::::::::a O:::::O     O:::::OS:::::S
       Z:::::Z      e::::::eeeee:::::eeaaaaaaaaa:::::aO:::::O     O:::::O S::::SSSS
      Z:::::Z      e::::::e     e:::::e         a::::aO:::::O     O:::::O  SS::::::SSSSS
     Z:::::Z       e:::::::eeeee::::::e  aaaaaaa:::::aO:::::O     O:::::O    SSS::::::::SS
    Z:::::Z        e:::::::::::::::::e aa::::::::::::aO:::::O     O:::::O       SSSSSS::::S
   Z:::::Z         e::::::eeeeeeeeeee a::::aaaa::::::aO:::::O     O:::::O            S:::::S
ZZZ:::::Z     ZZZZZe:::::::e         a::::a    a:::::aO::::::O   O::::::O            S:::::S
Z::::::ZZZZZZZZ:::Ze::::::::e        a::::a    a:::::aO:::::::OOO:::::::OSSSSSSS     S:::::S
Z:::::::::::::::::Z e::::::::eeeeeeeea:::::aaaa::::::a OO:::::::::::::OO S::::::SSSSSS:::::S
Z:::::::::::::::::Z  ee:::::::::::::e a::::::::::aa:::a  OO:::::::::OO   S:::::::::::::::SS
ZZZZZZZZZZZZZZZZZZZ    eeeeeeeeeeeeee  aaaaaaaaaa  aaaa    OOOOOOOOO      SSSSSSSSSSSSSSS`

func printStartup(s *Session) {
	fmt.Println(art)
	fmt.Println()
	fmt.Println("ZeaOS - Zero-copy Data REPL from Open Tempest Labs")
	fmt.Println()

	names := make([]string, 0, len(s.Registry))
	for name := range s.Registry {
		names = append(names, name)
	}
	tableList := "[]"
	if len(names) > 0 {
		tableList = "[" + strings.Join(names, ", ") + "]"
	}

	fmt.Printf("Tables: %s  Drive: %s  Plugins: %d\n\n",
		tableList, s.Drive.Label(), countPlugins())
}

func statusBar(s *Session) string {
	return fmt.Sprintf("Tables: %d | Drive: %s | Memory: %s",
		len(s.Registry), s.Drive.Label(), memUsage())
}

func countPlugins() int {
	home, err := os.UserHomeDir()
	if err != nil {
		return 0
	}
	entries, err := os.ReadDir(filepath.Join(home, ".zea", "plugins"))
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if info, err := e.Info(); err == nil && info.Mode()&0o111 != 0 {
			n++
		}
	}
	return n
}
