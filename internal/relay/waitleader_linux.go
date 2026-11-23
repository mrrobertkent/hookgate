//go:build linux

package relay

import (
	"errors"
	"os/exec"

	"golang.org/x/sys/unix"
)

// waitLeader blocks until the handler's leader process has exited, without reaping it. While the leader is an
// unreaped zombie its process ID, which is also its group's ID, cannot be reused, so killing the group then
// cannot reach an unrelated process. It reports false when the wait is not possible.
func waitLeader(cmd *exec.Cmd) bool {
	for {
		var info unix.Siginfo
		err := unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if !errors.Is(err, unix.EINTR) {
			return err == nil
		}
	}
}
