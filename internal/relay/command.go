package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"time"

	"github.com/mrrobertkent/hookgate/internal/contract"
)

// stderrTail bounds the handler output kept for logs.
const stderrTail = 4 << 10

// waitDelay bounds how long a finished or killed handler's output pipes are read after it exits.
const waitDelay = 5 * time.Second

// Exit codes with a meaning of their own; every other non-zero exit retries.
const (
	exitDataErr    = 65 // EX_DATAERR: the handler rejects the event
	exitConfig     = 78 // EX_CONFIG: the handler is misconfigured
	exitCannotExec = 126
	exitNotFound   = 127
)

// commandDeliverer runs the configured program once per event.
type commandDeliverer struct {
	cfg CommandHandler
	env []string // env_allow copies and set_env values, fixed at start
	log *slog.Logger
}

// newCommandDeliverer resolves set_env secrets once, at start, and checks that they and the arguments fit an execve.
func newCommandDeliverer(cfg CommandHandler, log *slog.Logger) (*commandDeliverer, error) {
	var env []string
	for _, name := range cfg.EnvAllow {
		if v, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+v)
		}
	}
	names := make([]string, 0, len(cfg.SetEnv))
	for name := range cfg.SetEnv {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		ref := cfg.SetEnv[name]
		v, err := ref.Resolve()
		if err != nil {
			return nil, fmt.Errorf("handler.command.set_env.%s: %w", name, err)
		}
		if v != "" || !ref.Optional {
			env = append(env, name+"="+v)
		}
	}
	if err := checkExecSize(cfg.Path, cfg.Args, env, currentExecLimits()); err != nil {
		return nil, err
	}
	return &commandDeliverer{cfg: cfg, env: env, log: log}, nil
}

func (c *commandDeliverer) deliver(ctx context.Context, ev *event) result {
	runCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	cmd := exec.Command(c.cfg.Path, c.cfg.Args...) //nolint:gosec // G204: the operator configures the program; event data only reaches its stdin and environment.
	cmd.Env = append(slices.Clone(c.env), eventEnv(ev)...)
	cmd.Stdin = bytes.NewReader(ev.body)
	stderr := &tailBuffer{max: stderrTail}
	cmd.Stderr = stderr
	cmd.WaitDelay = waitDelay
	isolate(cmd)
	err := run(runCtx, cmd)
	res := c.classify(ctx, runCtx, cmd, err)
	if res.verdict != verdictAck {
		c.log.Warn("handler failed", "seq", ev.streamSeq, "attempt", ev.attempt, "result", res.verdict.String(),
			"detail", res.detail, "stderr", stderr.String())
	}
	return res
}

// run starts cmd and waits for it. When ctx ends first, the whole process group is killed, whatever state the
// output pipes are in. Once the leader exits, the rest of its group is killed too, so nothing a handler starts
// outlives it.
func run(ctx context.Context, cmd *exec.Cmd) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	watcher := make(chan struct{})
	go func() {
		defer close(watcher)
		select {
		case <-ctx.Done():
			killGroup(cmd)
		case <-done:
		}
	}()
	if waitLeader(cmd) {
		// The leader has exited and is not yet reaped, so its group ID still names this handler's group.
		close(done)
		<-watcher
		killGroup(cmd)
		return cmd.Wait()
	}
	err := cmd.Wait()
	close(done)
	<-watcher
	killGroup(cmd)
	return err
}

func (c *commandDeliverer) classify(ctx, runCtx context.Context, cmd *exec.Cmd, err error) result {
	if err == nil {
		return result{verdict: verdictAck}
	}
	if cmd.Process == nil {
		return startResult(c.cfg.Path, err)
	}
	if ctx.Err() != nil {
		return result{verdict: verdictCancelled, detail: "cancelled by shutdown"}
	}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return resultf(verdictRetry, "timed out after %s", c.cfg.Timeout)
	}
	if errors.Is(err, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success() {
		return result{verdict: verdictAck}
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return resultf(verdictRetry, "wait: %v", err)
	}
	if sig, ok := signalled(ee); ok {
		return resultf(verdictRetry, "killed by signal %s", sig)
	}
	return exitResult(ee.ExitCode())
}

// exitResult maps an exit status. An unknown failure retries: runtimes exit 1 or 2 on an unhandled error, which is
// as likely transient as not, so only an explicit 65 rejects. The detail never carries handler output, which may
// hold secrets or body data.
func exitResult(code int) result {
	detail := "exit status " + strconv.Itoa(code)
	switch code {
	case exitDataErr:
		return result{verdict: verdictReject, detail: detail}
	case exitConfig, exitCannotExec, exitNotFound:
		return result{verdict: verdictPause, detail: detail}
	}
	return result{verdict: verdictRetry, detail: detail}
}

// startResult maps a start error: a transient resource shortage retries, an environment too large for this event
// rejects it, and anything else means the program cannot run at all.
func startResult(path string, err error) result {
	cause := unwrapPathError(err)
	switch startFailure(err) {
	case startTransient:
		return resultf(verdictRetry, "start %s: %v", path, cause)
	case startTooLarge:
		return resultf(verdictReject, "start %s: %v", path, cause)
	}
	return resultf(verdictPause, "cannot execute %s: %v", path, cause)
}

func unwrapPathError(err error) error {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}

// eventEnv is the per-event environment of a command handler.
func eventEnv(ev *event) []string {
	headers, _ := json.Marshal(ev.headers) // map[string][]string always marshals
	return []string{
		"HOOKGATE_CONTRACT=" + ev.get(contract.HeaderContract),
		"HOOKGATE_SOURCE=" + ev.get(contract.HeaderSource),
		"HOOKGATE_SUBJECT=" + ev.subject,
		"HOOKGATE_MESSAGE_ID=" + ev.msgID,
		"HOOKGATE_EVENT_ID=" + ev.get(contract.HeaderEventID),
		"HOOKGATE_EVENT_ID_AUTHENTICATED=" + ev.get(contract.HeaderEventIDAuthenticated),
		"HOOKGATE_ATTEMPT=" + strconv.FormatUint(ev.attempt, 10),
		"HOOKGATE_STREAM_SEQ=" + strconv.FormatUint(ev.streamSeq, 10),
		"HOOKGATE_REPLAY=" + ev.get(contract.HeaderReplay),
		"HOOKGATE_RECEIVED_AT=" + ev.get(contract.HeaderReceivedAt),
		"HOOKGATE_HEADERS=" + string(headers),
	}
}
