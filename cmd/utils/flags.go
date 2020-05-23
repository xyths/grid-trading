package utils

import "github.com/urfave/cli/v2"

// common flags for cmd
var (
	ConfigFlag = &cli.StringFlag{
		Name:    "config",
		Aliases: []string{"c"},
		Value:   "config.json",
		Usage:   "load configuration from `file`",
	}
)
