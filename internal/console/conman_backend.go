// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
	"github.com/creack/pty"
)

// conmanConsoleIO implements consoleIO for IPMI nodes managed by conman.
// It runs a goroutine that keeps a conman process alive, reconnecting automatically.
type conmanConsoleIO struct {
	nodeID string

	mu   sync.Mutex
	cmd  *exec.Cmd
	ptmx *os.File

	out    chan []byte
	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
}

// newConmanConsoleIO creates and starts a conmanConsoleIO for nodeID.
func newConmanConsoleIO(nodeID string) (*conmanConsoleIO, error) {
	if !nodes.IsCurrentNode(nodeID) {
		return nil, fmt.Errorf("node %s is not a current node", nodeID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &conmanConsoleIO{
		nodeID: nodeID,
		out:    make(chan []byte, 256),
		ctx:    ctx,
		cancel: cancel,
	}

	go c.run()
	return c, nil
}

// run is the internal goroutine that manages the conman process lifecycle.
func (c *conmanConsoleIO) run() {
	defer close(c.out)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if err := c.startProcess(); err != nil {
			slog.Error("Failed to start conman process", "nodeID", c.nodeID, "error", err)
			select {
			case <-time.After(time.Second):
			case <-c.ctx.Done():
				return
			}
			continue
		}

		c.readLoop()

		// Process exited. Clean up.
		c.mu.Lock()
		c.ptmx = nil
		c.cmd = nil
		c.mu.Unlock()

		// Notify the client and wait before reconnecting.
		reconnectMsg := fmt.Sprintf("\n[Reconnecting to %s...]\n", c.nodeID)
		select {
		case c.out <- []byte(reconnectMsg):
		case <-c.ctx.Done():
			return
		}

		select {
		case <-time.After(time.Second):
		case <-c.ctx.Done():
			return
		}

		if !nodes.IsCurrentNode(c.nodeID) {
			slog.Info("Node no longer in inventory, stopping conman backend", "nodeID", c.nodeID)
			return
		}
	}
}

// startProcess launches a new conman process with a PTY.
func (c *conmanConsoleIO) startProcess() error {
	select {
	case <-c.ctx.Done():
		return fmt.Errorf("context cancelled")
	default:
	}

	cmd := exec.Command("conman", c.nodeID)
	ptmx, err := pty.Start(cmd)
	if err != nil {
		return fmt.Errorf("pty.Start conman: %w", err)
	}

	c.mu.Lock()
	c.cmd = cmd
	c.ptmx = ptmx
	c.mu.Unlock()

	// Reap the child process to prevent zombies.
	go func() {
		if err := cmd.Wait(); err != nil {
			slog.Debug("conman process exited", "nodeID", c.nodeID, "error", err)
		}
	}()

	return nil
}

// readLoop reads PTY output and sends it to the output channel.
func (c *conmanConsoleIO) readLoop() {
	buf := make([]byte, 4096)
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.mu.Lock()
		ptmx := c.ptmx
		c.mu.Unlock()

		if ptmx == nil {
			return
		}

		ready, err := waitForPTYReadable(int(ptmx.Fd()), 250*time.Millisecond)
		if err != nil {
			return
		}
		if !ready {
			continue
		}

		n, err := ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			select {
			case c.out <- data:
			case <-c.ctx.Done():
				return
			}
		}
		if err != nil {
			if err.Error() != "EOF" && !isEIO(err) {
				slog.Error("PTY read error", "nodeID", c.nodeID, "error", err)
			}
			return
		}
	}
}

func (c *conmanConsoleIO) Output() <-chan []byte { return c.out }

func (c *conmanConsoleIO) Write(p []byte) (int, error) {
	c.mu.Lock()
	ptmx := c.ptmx
	c.mu.Unlock()

	if ptmx == nil {
		return len(p), nil // silent drop during reconnect
	}
	return ptmx.Write(p)
}

func (c *conmanConsoleIO) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		ptmx := c.ptmx
		cmd := c.cmd
		c.mu.Unlock()

		if ptmx != nil {
			// Send ConMan escape to gracefully disconnect.
			_, _ = ptmx.Write([]byte("&."))
			time.Sleep(100 * time.Millisecond)
			ptmx.Close()
		}
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}

		c.cancel()
	})
	return nil
}

// isEIO checks if an error is EIO (expected when a PTY process exits).
func isEIO(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.EIO
	}
	return false
}

// waitForPTYReadable waits until the PTY fd is readable or the timeout elapses.
func waitForPTYReadable(fd int, timeout time.Duration) (bool, error) {
	var readSet unix.FdSet
	readSet.Zero()
	readSet.Set(fd)

	tv := unix.NsecToTimeval(timeout.Nanoseconds())
	n, err := unix.Select(fd+1, &readSet, nil, nil, &tv)
	if err != nil {
		if errors.Is(err, syscall.EINTR) {
			return false, nil
		}
		return false, err
	}
	return n > 0, nil
}
