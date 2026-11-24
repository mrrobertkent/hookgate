package relay

import (
	"fmt"
	"strconv"
	"strings"
)

// execLimits are the kernel's limits on what execve copies: each argument and environment string, and all of
// them together with their pointers. A zero limit is not checked.
type execLimits struct {
	maxString int // longest string, terminating NUL included (MAX_ARG_STRLEN)
	total     int // strings, NULs and pointers together
}

// Upper bounds of the per-event environment (eventEnv): HOOKGATE_HEADERS at the broker's 64 KiB header limit, and
// the other variables, whose values the contract keeps to a few hundred bytes each.
const (
	eventEnvVars        = 11
	maxHeadersJSONBytes = 64 << 10
	maxEventScalarBytes = 4 << 10
)

// pointerSize is the size of one argv or envp pointer on the 64-bit systems the relay runs on.
const pointerSize = 8

// execSlack covers the stack slot below the strings and rounding.
const execSlack = 4 << 10

// An interpreter (a #! script or binfmt_misc) adds strings to the same budget: the program's path again, the
// interpreter's path and an argument. The kernel follows at most 5 interpreters; binfmt_misc registrations, the
// longer kind, are under 2 KiB.
const (
	maxInterpreters      = 5
	maxInterpreterString = 2 << 10
)

// fixedOverhead is what an execve of path copies besides argv and envp: the file name, and what interpreters add.
func fixedOverhead(path string) int {
	return len(path) + 1 + maxInterpreters*(len(path)+1+maxInterpreterString+2*pointerSize) + execSlack
}

// eventEnvReserve is the most the per-event environment can add to an execve, pointers included.
func eventEnvReserve() int {
	return maxHeadersJSONBytes + maxEventScalarBytes + eventEnvVars*pointerSize
}

// checkExecSize rejects fixed arguments and environment that leave no room for the per-event variables, so that
// an execve can fail as too large only because of one event's own headers.
func checkExecSize(path string, args, env []string, lim execLimits) error {
	strs := make([]string, 0, 1+len(args)+len(env))
	where := make([]string, 0, cap(strs))
	strs, where = append(strs, path), append(where, "handler.command.path")
	for i, a := range args {
		strs, where = append(strs, a), append(where, "handler.command.args["+strconv.Itoa(i)+"]")
	}
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		strs, where = append(strs, kv), append(where, "handler.command environment variable "+name)
	}
	size := fixedOverhead(path)
	for i, s := range strs {
		n := len(s) + 1
		if strings.IndexByte(s, 0) >= 0 {
			return fmt.Errorf("%s contains a NUL byte", where[i])
		}
		if lim.maxString > 0 && n > lim.maxString {
			return fmt.Errorf("%s: %d bytes, above the %d-byte limit for one argument or environment string", where[i], len(s), lim.maxString-1)
		}
		size += n + pointerSize
	}
	if lim.total > 0 && size+eventEnvReserve() > lim.total {
		return fmt.Errorf("handler.command: arguments and environment take %d bytes; with up to %d bytes of HOOKGATE_ variables per event they exceed the %d-byte limit of one execve (a quarter of the stack size limit)",
			size, eventEnvReserve(), lim.total)
	}
	return nil
}
