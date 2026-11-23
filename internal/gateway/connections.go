package gateway

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/mrrobertkent/hookgate/internal/natsconn"
	"github.com/mrrobertkent/hookgate/internal/output"
)

// watchers maps a connection name to the outputs to tell when it (re)connects.
type watchers struct {
	mu     sync.Mutex
	byConn map[string][]output.Watcher
}

func (w *watchers) add(conn string, o output.Watcher) {
	w.mu.Lock()
	w.byConn[conn] = append(w.byConn[conn], o)
	w.mu.Unlock()
}

func (w *watchers) list(conn string) []output.Watcher {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]output.Watcher(nil), w.byConn[conn]...)
}

func (w *watchers) state(conn string, up bool) {
	if up {
		return // Connected follows and starts a new check
	}
	for _, o := range w.list(conn) {
		o.Disconnected()
	}
}

func (w *watchers) reconnected(conn string) {
	for _, o := range w.list(conn) {
		o.Reconnected()
	}
}

// connect opens every connection a source's output uses, once per name. It returns the configuration fault of
// each connection that could not be set up; the client keeps retrying servers that are only unreachable.
func (g *Gateway) connect() (map[string]error, *watchers) {
	ws := &watchers{byConn: map[string][]output.Watcher{}}
	errs := map[string]error{}
	g.conns = map[string]*natsconn.Conn{}
	for _, s := range g.cfg.Sources {
		js := s.Output.JetStream
		if js == nil {
			continue
		}
		name := js.Connection
		if _, done := g.conns[name]; done || errs[name] != nil {
			continue
		}
		g.m.connected.SetBool(false, name)
		c, err := natsconn.Connect("hookgate-"+name, *g.cfg.Connections[name].NATS, g.log, natsconn.Callbacks{
			Connected: func(*nats.Conn) { ws.reconnected(name) },
			State: func(up bool) {
				g.m.connected.SetBool(up, name)
				ws.state(name, up)
			},
		})
		if err != nil {
			errs[name] = err
			continue
		}
		g.conns[name] = c
	}
	return errs, ws
}

// Close stops the output watchers and drains every connection, waiting until each is closed or ctx ends.
// In-flight deliveries must have finished first.
func (g *Gateway) Close(ctx context.Context) error {
	g.stop()
	names := make([]string, 0, len(g.conns))
	for n := range g.conns {
		names = append(names, n)
	}
	sort.Strings(names)
	errs := make([]error, len(names))
	var wg sync.WaitGroup
	for i, n := range names {
		wg.Go(func() { errs[i] = g.conns[n].Drain(ctx) })
	}
	wg.Wait()
	return errors.Join(errs...)
}
