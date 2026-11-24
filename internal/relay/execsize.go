package relay

import (
	"fmt"
	"strconv"
	"strings"
)

// execLimits are the kernel's limits on what execve copies: each argument and environment string, and all of
// them together with their pointers. Nothing is checked unless known.
type execLimits struct {
	known     bool
	maxString int // longest string, terminating NUL included (MAX_ARG_STRLEN)
	total     int // strings, NULs and pointers together
	subject   int // the longest configured subject, which HOOKGATE_SUBJECT may carry
}

// Upper bounds of the per-event environment (eventEnv): 64 KiB of HOOKGATE_HEADERS JSON, the subject, and the
// other variables, whose values the contract keeps to a few hundred bytes each.
const (
	eventEnvVars        = 11
	maxHeadersJSONBytes = 64 << 10
	maxEventScalarBytes = 4 << 10
)

// pointerSize is the size of one argv or envp pointer on the 64-bit systems the relay runs on.
const pointerSize = 8

// execSlack covers the stack slot below the strings and rounding.
const execSlack = 4 << 10

// An interpreter (a #! script or binfmt_misc) adds strings to the same budget: the path it was given (the
// program's, or the previous interpreter's), its own path and an argument. The kernel follows at most 5
// interpreters; binfmt_misc registrations, the longer kind, are under 2 KiB.
const (
	maxInterpreters      = 5
	maxInterpreterString = 2 << 10
)

// fixedOverhead is what an execve of path copies besides argv and envp: the file name, and what interpreters add.
func fixedOverhead(path string) int {
	perInterpreter := max(len(path)+1, maxInterpreterString) + 2*maxInterpreterString + 3*pointerSize
	return len(path) + 1 + maxInterpreters*perInterpreter + execSlack
}

// reserve is the room kept for the per-event environment, pointers included.
func (lim execLimits) reserve() int {
	return maxHeadersJSONBytes + maxEventScalarBytes + lim.subject + eventEnvVars*pointerSize
}

// envSize is what strings add to an execve, NULs and pointers included.
func envSize(strs []string) int {
	n := 0
	for _, s := range strs {
		n += len(s) + 1 + pointerSize
	}
	return n
}

// withinReserve reports whether an event's environment fits what the start check reserved for it, so that an
// execve failing as too large is not the event's doing.
func (lim execLimits) withinReserve(eventEnv []string) bool {
	if !lim.known {
		return false
	}
	for _, s := range eventEnv {
		if len(s)+1 > lim.maxString {
			return false
		}
	}
	return envSize(eventEnv) <= lim.reserve()
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
		if lim.known && n > lim.maxString {
			return fmt.Errorf("%s: %d bytes, above the %d-byte limit for one argument or environment string", where[i], len(s), lim.maxString-1)
		}
		size += n + pointerSize
	}
	if lim.known && size+lim.reserve() > lim.total {
		return fmt.Errorf("handler.command: arguments and environment take %d bytes; with up to %d bytes of HOOKGATE_ variables per event they exceed the %d-byte limit of one execve (a quarter of the stack size limit)",
			size, lim.reserve(), lim.total)
	}
	return nil
}
