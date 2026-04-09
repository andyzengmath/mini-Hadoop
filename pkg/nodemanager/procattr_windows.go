//go:build windows

package nodemanager

import (
	"os/exec"
	"syscall"
)

// setSysProcAttr sets process creation flags on Windows for process group management.
func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}
