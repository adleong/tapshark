package main

import (
	"os"
	"github.com/adleong/tapshark/cmd"
)

func main() {
	if err := cmd.NewCmdTapShark().Execute(); err != nil {
		os.Exit(1)
	}
}
