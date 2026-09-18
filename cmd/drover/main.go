// Command drover is a set of tenancy extensions for Rancher.
package main

import (
	"fmt"
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
		usage()
		return 2
	}

	switch args[0] {
	case "--version":
		fmt.Println(version)
		return 0
	case "-h", "--help":
		usage()
		return 0
	}

	cmd, ok := subcommands[args[0]]
	if !ok {
		usage()
		return 2
	}
	return cmd(args[1:])
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: drover <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "       drover --version")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  namespace-filter   Filter the namespace list of a Rancher project member.")
}
