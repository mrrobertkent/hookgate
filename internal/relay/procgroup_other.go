//go:build !unix

package relay

import "os/exec"

func isolate(*exec.Cmd) {}

func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func signalled(*exec.ExitError) (string, bool) { return "", false }

type startKind int

const (
	startPermanent startKind = iota
	startTransient
	startTooLarge
)

func startFailure(error) startKind { return startPermanent }
