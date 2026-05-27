//go:build !windows

package testsuite

import (
	"os"
	"os/exec"
)

// newCmd creates a new command with the given executable path and arguments.
func newCmd(exePath string, args ...string) *exec.Cmd { _ = "STUB: not implemented"; return nil }

// sendInterrupt sends an interrupt signal to the given process for graceful shutdown.
func sendInterrupt(process *os.Process) error { _ = "STUB: not implemented"; return nil }
