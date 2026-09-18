// Command drover is a set of tenancy extensions for Rancher.
package main

import (
	"fmt"
	"io"
	"os"
)

var version = "dev"

var subcommands = map[string]func(args []string) int{
	"namespace-filter": runNamespaceFilter,
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return 2
	}

	switch args[0] {
	case "--version":
		fmt.Println(version)
		return 0
	case "-h", "--help":
		usage(os.Stderr)
		return 0
	}

	cmd, ok := subcommands[args[0]]
	if !ok {
		usage(os.Stderr)
		return 2
	}
	return cmd(args[1:])
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "Usage: drover <subcommand> [flags]")
	fmt.Fprintln(w, "       drover --version")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Subcommands:")
	fmt.Fprintln(w, "  namespace-filter   Filter the namespace list of a Rancher project member.")
}
