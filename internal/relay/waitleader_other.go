//go:build !linux

package relay

import "os/exec"

// waitLeader is unavailable without waitid(WNOWAIT) (Linux only in golang.org/x/sys); the caller then kills the
// group after reaping the leader, when its ID could in principle have been reused.
func waitLeader(*exec.Cmd) bool { return false }
