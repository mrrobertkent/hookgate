package testutil

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Proxy forwards TCP connections to a target. It can hold back what the target sends, to keep replies pending
// or make them arrive after the client stopped waiting, and report what the client sent.
type Proxy struct {
	ln     net.Listener
	target string
	delay  atomic.Int64
	wg     sync.WaitGroup

	mu      sync.Mutex
	conns   []net.Conn
	hold    []byte // the next target read containing it is held back by holdFor
	holdFor time.Duration
	watches []watch
}

type watch struct {
	match []byte
	seen  chan struct{}
}

// StartProxy listens on a random loopback port and forwards each connection to target until the test ends.
func StartProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", loopback+":0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: target}
	p.wg.Go(p.accept)
	t.Cleanup(func() {
		ln.Close()
		p.mu.Lock()
		for _, c := range p.conns {
			c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
	return p
}

// Addr is the proxy's listening address.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// SetDelay holds back each read from the target by d before passing it on; 0 passes it at once.
func (p *Proxy) SetDelay(d time.Duration) { p.delay.Store(int64(d)) }

// HoldNext holds back the next read from the target that contains match by d, once.
func (p *Proxy) HoldNext(match []byte, d time.Duration) {
	p.mu.Lock()
	p.hold, p.holdFor = match, d
	p.mu.Unlock()
}

// Seen returns a channel that is closed once the client has sent bytes containing match.
func (p *Proxy) Seen(match []byte) <-chan struct{} {
	w := watch{match: match, seen: make(chan struct{})}
	p.mu.Lock()
	p.watches = append(p.watches, w)
	p.mu.Unlock()
	return w.seen
}

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		u, err := net.Dial("tcp", p.target)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, u)
		p.mu.Unlock()
		p.wg.Go(func() { p.pipe(u, c, p.observe); u.Close() })
		p.wg.Go(func() { p.pipe(c, u, p.wait); c.Close() })
	}
}

// pipe copies src to dst, calling before with each chunk before writing it.
func (p *Proxy) pipe(dst, src net.Conn, before func([]byte)) {
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			before(buf[:n])
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *Proxy) observe(b []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.watches[:0]
	for _, w := range p.watches {
		if bytes.Contains(b, w.match) {
			close(w.seen)
			continue
		}
		kept = append(kept, w)
	}
	p.watches = kept
}

func (p *Proxy) wait(b []byte) {
	d := time.Duration(p.delay.Load())
	p.mu.Lock()
	if p.hold != nil && bytes.Contains(b, p.hold) {
		d += p.holdFor
		p.hold = nil
	}
	p.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
}
