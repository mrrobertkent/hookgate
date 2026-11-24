//go:build unix

package relay

import (
	"errors"
	"os/exec"
	"syscall"
)

// isolate runs cmd in its own process group, so that killing it reaches everything it started and signals sent
// to the relay do not reach it.
func isolate(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killGroup kills cmd's process group; an empty group (ESRCH) is not an error. A group keeps its ID while any
// member lives, so a live ID still names this handler's group.
func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}

// signalled reports whether a finished command was killed by a signal, and which.
func signalled(err *exec.ExitError) (string, bool) {
	ws, ok := err.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() { //nolint:misspell // syscall API name.
		return "", false
	}
	return ws.Signal().String(), true
}

type startKind int

const (
	startPermanent startKind = iota
	startTransient
	startTooLarge
)

// startFailure classifies an error starting a program.
func startFailure(err error) startKind {
	switch {
	case errors.Is(err, syscall.EAGAIN), errors.Is(err, syscall.ENOMEM), errors.Is(err, syscall.ETXTBSY),
		errors.Is(err, syscall.EMFILE), errors.Is(err, syscall.ENFILE), errors.Is(err, syscall.EINTR):
		return startTransient
	case errors.Is(err, syscall.E2BIG):
		return startTooLarge
	}
	return startPermanent
}
