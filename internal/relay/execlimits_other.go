//go:build !linux

package relay

// currentExecLimits checks nothing: command handlers are supported on Linux only.
func currentExecLimits() execLimits { return execLimits{} }
