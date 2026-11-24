package gateway

import (
	"testing"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/natsconn"
)

// fakeStatus is a sampled status the test sets.
type fakeStatus struct{ st natsconn.Status }

func (f *fakeStatus) Status() natsconn.Status { return f.st }

func TestReadinessGrace(t *testing.T) {
	grace := 5 * time.Second
	now := time.Unix(1_700_000_000, 0)
	g := &Gateway{
		cfg:   &config.Config{Readiness: config.Readiness{RequireOutputConnections: true, DisconnectGrace: &grace}},
		m:     newMetrics(),
		links: newLinks(),
	}
	g.links.now = func() time.Time { return now }
	st := map[string]*fakeStatus{}
	for _, name := range []string{"a", "b", "unused"} {
		st[name] = &fakeStatus{natsconn.Status{Connected: true}}
		g.links.add(name, st[name])
	}
	g.links.use("a")
	g.links.use("b")
	expect := func(want string) {
		t.Helper()
		if got := g.notReady(); got != want {
			t.Errorf("notReady() = %q, want %q", got, want)
		}
	}
	down := func(name string, since time.Duration) { st[name].st = natsconn.Status{DownSince: now.Add(-since)} }

	expect("")
	down("b", grace)
	expect("") // exactly the grace
	down("b", grace+time.Millisecond)
	expect("connection b disconnected")
	st["b"].st = natsconn.Status{Rejected: true}
	expect("") // rejected credentials are a configuration fault
	down("a", time.Minute)
	down("b", time.Minute)
	down("unused", time.Hour)
	expect("connection a disconnected")
	st["a"].st, st["b"].st = natsconn.Status{Connected: true}, natsconn.Status{Connected: true}
	expect("") // "unused" is still down, but no output delivers through it

	down("a", time.Minute)
	g.cfg.Readiness.RequireOutputConnections = false
	expect("")
	g.m.draining.Store(true)
	expect("draining")
}
