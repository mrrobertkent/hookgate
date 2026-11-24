// Command hookgate-relay delivers webhook events from a NATS JetStream consumer to an application handler.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/natsconn"
	"github.com/mrrobertkent/hookgate/internal/relay"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `hookgate-relay delivers webhook events from a NATS JetStream consumer to an application handler.

Usage:
  hookgate-relay [serve] [-config FILE]               run the relay (default command)
  hookgate-relay validate [-config FILE]              check the configuration, secrets and handler program
  hookgate-relay healthcheck [-url URL]               exit 0 when the admin /healthz answers 200 (for container probes)
  hookgate-relay replay -seq N [-seq M] [-config FILE] [-claim-reference]
                                                      republish dead-letter records to the replay subject
                                                      of the consumer that failed them
  hookgate-relay version                              print the version
`

// connectWait bounds how long replay waits for the first connection.
const connectWait = 30 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// options are the flags of every subcommand.
type options struct {
	config         string
	url            string
	seqs           seqList
	claimReference bool
}

func run(args []string, stdout, stderr io.Writer) int {
	cmd := "serve"
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		cmd, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	var o options
	fs.StringVar(&o.config, "config", envOr("HOOKGATE_RELAY_CONFIG", "/etc/hookgate/relay.yaml"), "configuration file")
	fs.StringVar(&o.url, "url", "http://127.0.0.1:9091/healthz", "healthcheck URL")
	fs.Var(&o.seqs, "seq", "dead-letter stream sequence to replay (repeatable)")
	fs.BoolVar(&o.claimReference, "claim-reference", false, "replay records without a body by republishing their claim-check reference")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	switch cmd {
	case "version":
		fmt.Fprintln(stdout, version)
		return 0
	case "healthcheck":
		return healthcheck(o.url, stderr)
	case "validate":
		return validate(o.config, stdout, stderr)
	case "replay":
		return replay(o, stdout, stderr)
	case "serve":
		return serve(o.config, stderr)
	}
	fmt.Fprint(stderr, usage)
	return 2
}

func healthcheck(url string, stderr io.Writer) int {
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, resp.Status)
		return 1
	}
	return 0
}

func validate(path string, stdout, stderr io.Writer) int {
	cfg, err := relay.LoadConfig(path)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	errs := []error{}
	if _, err := natsconn.Options(cfg.Connection.NATS); err != nil {
		errs = append(errs, fmt.Errorf("connection: %w", err))
	}
	if _, err := relay.New(cfg, slog.New(slog.DiscardHandler)); err != nil {
		errs = append(errs, err)
	}
	if h := cfg.Handler.Command; h != nil {
		if err := checkExecutable(h.Path); err != nil {
			errs = append(errs, fmt.Errorf("handler.command.path: %w", err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "ok: consumer %s on stream %s\n", cfg.Consumer, cfg.Stream)
	return 0
}

// checkExecutable reports a handler program that is missing, not a regular file or not executable.
func checkExecutable(path string) error {
	fi, err := os.Stat(path)
	switch {
	case err != nil:
		return err
	case !fi.Mode().IsRegular():
		return fmt.Errorf("%s is not a regular file", path)
	case fi.Mode().Perm()&0o111 == 0:
		return fmt.Errorf("%s is not executable", path)
	}
	return nil
}

func serve(path string, stderr io.Writer) int {
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	var r *relay.Relay
	cfg, err := relay.LoadConfig(path)
	if err == nil {
		r, err = relay.New(cfg, log)
	}
	if err != nil {
		log.Error("config", "error", err)
		return 1
	}
	log.Info("starting", "version", version)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := r.Run(ctx); err != nil {
		log.Error("relay", "error", err)
		return 1
	}
	return 0
}

func replay(o options, stdout, stderr io.Writer) int {
	if len(o.seqs) == 0 {
		fmt.Fprintln(stderr, "replay: at least one -seq is required")
		return 2
	}
	cfg, err := relay.LoadConfig(o.config)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	log := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	nc, err := natsconn.Connect("relay-replay", cfg.Connection.NATS, log, natsconn.Callbacks{})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), connectWait)
	defer cancel()
	if werr := nc.WaitConnected(ctx); werr != nil {
		fmt.Fprintln(stderr, "replay: not connected to NATS:", werr)
		return 1
	}
	js, err := jetstream.New(nc.Conn)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	rp := relay.NewReplayer(cfg, js)
	rp.ClaimReference = o.claimReference
	return replayAll(rp, cfg.DeadLetter.Stream, o.seqs, stdout, stderr)
}

func replayAll(rp *relay.Replayer, dl string, seqs []uint64, stdout, stderr io.Writer) int {
	code := 0
	for _, seq := range seqs {
		ctx, cancel := context.WithTimeout(context.Background(), connectWait)
		res, err := rp.Replay(ctx, seq)
		cancel()
		if err != nil {
			if errors.Is(err, relay.ErrNoBody) {
				err = fmt.Errorf("%w; pass -claim-reference to republish the reference", err)
			}
			fmt.Fprintln(stderr, err)
			code = 1
			continue
		}
		note := ""
		if res.Duplicate {
			note = " (already replayed)"
		}
		fmt.Fprintf(stdout, "%s:%d -> %s, %s:%d%s\n", dl, seq, res.Subject, res.Stream, res.Sequence, note)
	}
	return code
}

// seqList is a repeatable -seq flag.
type seqList []uint64

func (s *seqList) String() string {
	parts := make([]string, len(*s))
	for i, v := range *s {
		parts[i] = strconv.FormatUint(v, 10)
	}
	return strings.Join(parts, ",")
}

func (s *seqList) Set(v string) error {
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || n == 0 {
		return errors.New("must be a stream sequence number >= 1")
	}
	*s = append(*s, n)
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
