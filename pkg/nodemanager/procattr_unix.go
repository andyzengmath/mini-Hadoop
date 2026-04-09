//go:build !windows

package nodemanager

import (
	"os/exec"
	"syscall"
)

// setSysProcAttr sets process group on Unix for clean child process cleanup.
func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
