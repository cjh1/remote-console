// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package console

// consoleIO abstracts the underlying console backend (PTY/conman or SSH).
// Implementations handle reconnection internally; Output() is closed only on
// permanent shutdown (context cancelled or node removed from inventory).
type consoleIO interface {
	// Output returns a channel of console output chunks. The channel is closed
	// when the backend shuts down permanently.
	Output() <-chan []byte
	// Write sends data to the console (user input).
	Write(p []byte) (n int, err error)
	// Close shuts down the backend. Safe to call multiple times.
	Close() error
}
