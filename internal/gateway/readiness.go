package gateway

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mrrobertkent/hookgate/internal/natsconn"
)

// statusSource is a connection's sampled status; *natsconn.Conn implements it without waiting on the client.
type statusSource interface {
	Status() natsconn.Status
}

// link is one output connection as /readyz sees it.
type link struct {
	src  statusSource
	used bool // at least one built output delivers through it
}

// links are the output connections /readyz considers. It reads only their sampled status.
type links struct {
	now func() time.Time

	mu     sync.Mutex
	byName map[string]*link
}

func newLinks() *links {
	return &links{now: time.Now, byName: map[string]*link{}}
}

// add registers an opened connection. A connection that could not be opened is a configuration fault and is
// never added.
func (l *links) add(name string, src statusSource) {
	l.mu.Lock()
	l.byName[name] = &link{src: src}
	l.mu.Unlock()
}

// use marks a connection as one an output delivers through.
func (l *links) use(name string) {
	l.mu.Lock()
	if k := l.byName[name]; k != nil {
		k.used = true
	}
	l.mu.Unlock()
}

// lost returns the first used connection, by name, that has been down longer than grace, or "". A connection
// whose credentials are rejected is a configuration fault shared by every instance and never counts.
func (l *links) lost(grace time.Duration) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	var names []string
	for name, k := range l.byName {
		if !k.used {
			continue
		}
		st := k.src.Status()
		if !st.Connected && !st.Rejected && !st.DownSince.IsZero() && now.Sub(st.DownSince) > grace {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return names[0]
}

// notReady returns why /readyz answers 503, or "" when it answers 200. Only conditions local to this
// instance count: a configuration fault is shared by every instance and fails its source, never readiness.
func (g *Gateway) notReady() string {
	if g.m.draining.Load() {
		return "draining"
	}
	if r := g.cfg.Readiness; r.RequireOutputConnections {
		if name := g.links.lost(*r.DisconnectGrace); name != "" {
			return fmt.Sprintf("connection %s disconnected", name)
		}
	}
	return ""
}
