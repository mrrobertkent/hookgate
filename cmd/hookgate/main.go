// Command hookgate verifies incoming webhooks and delivers only authentic requests to an HTTP upstream or a NATS JetStream stream.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/gateway"
	"github.com/mrrobertkent/hookgate/internal/natsconn"
	"github.com/mrrobertkent/hookgate/internal/verify"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `hookgate verifies incoming webhooks and delivers only authentic requests to an HTTP upstream or a NATS JetStream stream.

Usage:
  hookgate [serve] [-config FILE]        run the gateway (default command)
  hookgate validate [-config FILE]       check the configuration and that every secret is set
  hookgate healthcheck [-url URL]        exit 0 when the admin /healthz answers 200 (for container probes)
  hookgate version                       print the version
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	cmd := "serve"
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		cmd, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	cfgPath := fs.String("config", envOr("HOOKGATE_CONFIG", "/etc/hookgate/hookgate.yaml"), "configuration file")
	probeURL := fs.String("url", "http://127.0.0.1:9090/healthz", "healthcheck URL")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	switch cmd {
	case "version":
		fmt.Fprintln(stdout, version)
		return 0
	case "healthcheck":
		c := &http.Client{Timeout: 3 * time.Second}
		resp, err := c.Get(*probeURL) //nolint:gosec // G704: the operator sets the probe URL; it defaults to loopback.
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
	case "validate":
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if !secretsSet(cfg, stderr) {
			return 1
		}
		fmt.Fprintf(stdout, "ok: %d sources\n", len(cfg.Sources))
		return 0
	case "serve":
		log := slog.New(slog.NewJSONHandler(stderr, nil))
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			log.Error("config", "error", err)
			return 1
		}
		log.Info("starting", "version", version)
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		if err := gateway.New(cfg, log).Run(ctx); err != nil {
			log.Error("server", "error", err)
			return 1
		}
		return 0
	default:
		fmt.Fprint(stderr, usage)
		return 2
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// secretsSet reports whether every secret the configuration references resolves, printing each that does not.
func secretsSet(cfg *config.Config, stderr io.Writer) bool {
	var problems []string
	for name, c := range cfg.Connections {
		if c.NATS == nil {
			continue
		}
		if _, err := natsconn.Options(*c.NATS); err != nil {
			problems = append(problems, fmt.Sprintf("connection %q: %v", name, err))
		}
	}
	for _, s := range cfg.Sources {
		problems = append(problems, sourceSecretProblems(s)...)
	}
	for _, p := range problems {
		fmt.Fprintln(stderr, p)
	}
	return len(problems) == 0
}

func sourceSecretProblems(s config.Source) []string {
	var problems []string
	for _, c := range s.Checks {
		if _, err := verify.Build(c); err != nil {
			problems = append(problems, fmt.Sprintf("source %q: %v", s.Name, err))
		}
	}
	for name, ref := range setHeaders(s.Output) {
		if v, err := ref.Resolve(); err != nil || v == "" {
			problems = append(problems, fmt.Sprintf("source %q: set_headers %s: %s is unset or empty", s.Name, name, ref))
		}
	}
	return problems
}

func setHeaders(o config.Output) map[string]config.SecretRef {
	if o.HTTP != nil {
		return o.HTTP.SetHeaders
	}
	return o.JetStream.SetHeaders
}
