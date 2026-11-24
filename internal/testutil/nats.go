// Package testutil runs embedded NATS servers for tests. Only test files import it.
package testutil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// loopback is where every embedded server listens.
const loopback = "127.0.0.1"

// RunServer starts a single JetStream server on a random loopback port, stopped when the test ends.
// configure may adjust the options before the server starts.
func RunServer(t testing.TB, configure ...func(*server.Options)) *server.Server {
	t.Helper()
	opts := &server.Options{
		ServerName: "solo",
		Host:       loopback,
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		NoLog:      true,
		NoSigs:     true,
	}
	for _, c := range configure {
		c(opts)
	}
	return start(t, opts)
}

// RunServerFromConfig starts a server from NATS configuration text; "$STORE" in it is replaced by a temporary
// directory and the client port is random unless the text sets one.
func RunServerFromConfig(t testing.TB, conf string) *server.Server {
	t.Helper()
	dir := t.TempDir()
	conf = strings.ReplaceAll(conf, "$STORE", filepath.ToSlash(filepath.Join(dir, "js")))
	file := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(file, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Port == 0 {
		opts.Port = -1
	}
	opts.Host, opts.NoLog, opts.NoSigs = loopback, true, true
	return start(t, opts)
}

// Cluster is a JetStream cluster of embedded servers.
type Cluster struct {
	Servers []*server.Server
}

// RunCluster starts n clustered JetStream servers and waits until JetStream has a meta leader.
func RunCluster(t testing.TB, n int) *Cluster {
	t.Helper()
	clusterPorts := make([]int, n)
	for i := range clusterPorts {
		clusterPorts[i] = FreePort(t)
	}
	c := &Cluster{}
	for i := range n {
		var routes []*url.URL
		for j, p := range clusterPorts {
			if j != i {
				routes = append(routes, &url.URL{Scheme: "nats", Host: fmt.Sprintf("%s:%d", loopback, p)})
			}
		}
		opts := &server.Options{
			ServerName: fmt.Sprintf("n%d", i+1),
			Host:       loopback,
			Port:       -1,
			JetStream:  true,
			StoreDir:   t.TempDir(),
			NoLog:      true,
			NoSigs:     true,
			Cluster:    server.ClusterOpts{Name: "test", Host: loopback, Port: clusterPorts[i]},
			Routes:     routes,
		}
		s, err := server.NewServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		go s.Start()
		c.Servers = append(c.Servers, s)
		t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	}
	for _, s := range c.Servers {
		if !s.ReadyForConnections(10 * time.Second) {
			t.Fatalf("server %s did not start", s.Name())
		}
	}
	c.WaitForMetaLeader(t)
	return c
}

// URLs lists the client URLs of the servers still running.
func (c *Cluster) URLs() []string {
	var out []string
	for _, s := range c.Servers {
		if s.Running() {
			out = append(out, s.ClientURL())
		}
	}
	return out
}

// WaitForMetaLeader waits until a running server is the JetStream meta leader and knows every running server
// as a peer, so replicated assets can be placed.
func (c *Cluster) WaitForMetaLeader(t testing.TB) {
	t.Helper()
	Eventually(t, 20*time.Second, func() bool {
		running := 0
		for _, s := range c.Servers {
			if s.Running() {
				running++
			}
		}
		for _, s := range c.Servers {
			if s.Running() && s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) >= running {
				return true
			}
		}
		return false
	}, "no JetStream meta leader with every peer")
}

// StreamLeader returns the server leading stream in account $G.
func (c *Cluster) StreamLeader(stream string) *server.Server {
	for _, s := range c.Servers {
		if s.Running() && s.JetStreamIsStreamLeader(server.DEFAULT_GLOBAL_ACCOUNT, stream) {
			return s
		}
	}
	return nil
}

func start(t testing.TB, opts *server.Options) *server.Server {
	t.Helper()
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not start")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s
}

// Connect opens a plain client connection for test setup and inspection.
func Connect(t testing.TB, urls ...string) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return nc, js
}

// CreateStream creates a stream and fails the test on error.
func CreateStream(t testing.TB, js jetstream.JetStream, cfg jetstream.StreamConfig) jetstream.Stream {
	t.Helper()
	s, err := retryCreate(func(ctx context.Context) (jetstream.Stream, error) { return js.CreateStream(ctx, cfg) })
	if err != nil {
		t.Fatalf("create stream %s: %v", cfg.Name, err)
	}
	return s
}

// retryCreate repeats a create with short attempts: on a freshly formed cluster the first request can be lost
// while the meta leader settles, and one long attempt would wait for a reply that never comes. Creates are
// idempotent for an identical configuration.
func retryCreate[T any](create func(context.Context) (T, error)) (T, error) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		v, err := create(ctx)
		cancel()
		if err == nil || time.Now().After(deadline) || !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoStreamResponse) && !errors.Is(err, nats.ErrNoResponders) {
			return v, err
		}
	}
}

// CreateBucket creates an Object Store bucket and fails the test on error.
func CreateBucket(t testing.TB, js jetstream.JetStream, cfg jetstream.ObjectStoreConfig) jetstream.ObjectStore {
	t.Helper()
	o, err := retryCreate(func(ctx context.Context) (jetstream.ObjectStore, error) { return js.CreateObjectStore(ctx, cfg) })
	if err != nil {
		t.Fatalf("create bucket %s: %v", cfg.Bucket, err)
	}
	return o
}

// FreePort returns a loopback TCP port that was free a moment ago.
func FreePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Eventually polls cond every 20ms until it holds or timeout passes.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
