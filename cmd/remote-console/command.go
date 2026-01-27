package main

import (
	"context"
	"log"
	"strings"

	"github.com/urfave/cli/v3"
	"github.com/urfave/sflags"
	"github.com/urfave/sflags/gen/gcli"
)

func ensureTrailingSlash(url string) string {
	if url != "" && !strings.HasSuffix(url, "/") {
		return url + "/"
	}
	return url
}

func command(config *remoteConsoleConfig) *cli.Command {

	cmd := &cli.Command{
		Name:        "remote-console",
		Usage:       "access remote consoles",
		Description: "OpenCHAMI remote console service",
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			config.SmdURL = ensureTrailingSlash(config.SmdURL)

			return ctx, validateConfig(config)
		},
		Action: func(context.Context, *cli.Command) error {
			return runService(*config)
		},
	}

	err := gcli.ParseToV3(config, &cmd.Flags, sflags.EnvPrefix("RCS_"))
	if err != nil {
		log.Fatalf("err: %v", err)
	}

	// Add log config separately, so we can flatten it
	err = gcli.ParseToV3(&config.Log, &cmd.Flags, sflags.EnvPrefix("RCS_"))
	if err != nil {
		log.Fatalf("err: %v", err)
	}

	return cmd
}
