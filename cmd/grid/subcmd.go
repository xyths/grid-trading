package main

import (
	"github.com/urfave/cli/v2"
	"github.com/xyths/grid-trading/cmd/utils"
	"github.com/xyths/grid-trading/grid"
)

var (
	tradeCommand = &cli.Command{
		Action: trade,
		Name:   "trade",
		Usage:  "Trading with grid strategy",
		Subcommands: []*cli.Command{
			{
				Action: printGrid,
				Name:   "print",
				Usage:  "Print the grid generated by strategy parameters",
			},
		},
	}
)

func trade(ctx *cli.Context) error {
	configFile := ctx.String(utils.ConfigFlag.Name)
	g := grid.New(configFile)
	g.Init(ctx.Context)
	defer g.Close(ctx.Context)
	return g.Trade(ctx.Context)
}

func printGrid(ctx *cli.Context) error {
	configFile := ctx.String(utils.ConfigFlag.Name)
	g := grid.New(configFile)
	g.Init(ctx.Context)
	defer g.Close(ctx.Context)
	return g.Print(ctx.Context)
}
