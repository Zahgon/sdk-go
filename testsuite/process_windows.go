package testsuite

import (
	"os"
	"os/exec"
)

// newCmd creates a new command with the given executable path and arguments.
func newCmd(exePath string, args ...string) *exec.Cmd { _ = "STUB: not implemented"; return nil }

// isolate the process and signals sent to it from the current console

// sendInterrupt calls the break event on the given process for graceful shutdown.
func sendInterrupt(process *os.Process) error { _ = "STUB: not implemented"; return nil }
