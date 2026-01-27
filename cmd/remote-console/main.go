//
//  MIT License
//
//  (C) Copyright 2021-2023 Hewlett Packard Enterprise Development LP
//
//  Permission is hereby granted, free of charge, to any person obtaining a
//  copy of this software and associated documentation files (the "Software"),
//  to deal in the Software without restriction, including without limitation
//  the rights to use, copy, modify, merge, publish, distribute, sublicense,
//  and/or sell copies of the Software, and to permit persons to whom the
//  Software is furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included
//  in all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
//  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
//  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
//  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//  OTHER DEALINGS IN THE SOFTWARE.
//

// This file handles command line entry and enables the http service.

package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// initLogger sets up logging with slog
func initLogger() {
	// Determine log level from environment
	level := slog.LevelInfo
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		if err := level.UnmarshalText([]byte(logLevel)); err != nil {
			// If parsing fails, keep default INFO level
			slog.Warn("Invalid LOG_LEVEL, using INFO", "value", logLevel, "error", err)
		}
	}

	// Determine format from environment (json or text)
	format := strings.ToLower(os.Getenv("LOG_FORMAT"))
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		// Default to text format for development
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}

	slog.SetDefault(slog.New(handler))
	slog.Info("Logger initialized", "level", level.String(), "format", format)
}

func main() {
	initLogger()

	config := DefaultConfig()
	cli := command(&config)

	if err := cli.Run(context.Background(), os.Args); err != nil {
		slog.Error("Service failed", "error", err)
		os.Exit(1)
	}
}
