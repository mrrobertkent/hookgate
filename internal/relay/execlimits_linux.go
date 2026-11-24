//go:build linux

package relay

import (
	"os"

	"golang.org/x/sys/unix"
)

// Linux constants from fs/exec.c and include/uapi/linux/limits.h.
const (
	linuxArgMax = 128 << 10 // ARG_MAX: the floor of the total, whatever the stack limit
	linuxStkLim = 8 << 20   // _STK_LIM: the total is at most three quarters of it
)

// currentExecLimits are the limits an execve from this process meets: strings of at most 32 pages, and a total of
// a quarter of the stack size limit, between ARG_MAX and three quarters of _STK_LIM, but no more than the stack
// itself can hold, or ARG_MAX when the limit cannot be read.
func currentExecLimits() execLimits {
	page := os.Getpagesize()
	total := linuxStkLim / 4 * 3
	var rl unix.Rlimit
	switch err := unix.Getrlimit(unix.RLIMIT_STACK, &rl); {
	case err != nil:
		total = linuxArgMax // assume the smallest stack rather than the largest
	case rl.Cur != unix.RLIM_INFINITY:
		total = int(min(uint64(total), rl.Cur/4))
		// The ARG_MAX floor holds only if the stack can grow that far.
		return execLimits{maxString: 32 * page, total: min(max(total, linuxArgMax), int(min(rl.Cur, linuxStkLim))&^(page-1))}
	}
	return execLimits{maxString: 32 * page, total: max(total, linuxArgMax)}
}
