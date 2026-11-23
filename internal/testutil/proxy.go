package testutil

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Proxy forwards TCP connections to a target and can hold back what the target sends, to keep replies pending.
type Proxy struct {
	ln     net.Listener
	target string
	delay  atomic.Int64
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  []net.Conn
}

// StartProxy listens on a random loopback port and forwards each connection to target until the test ends.
func StartProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
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
		p.wg.Go(func() { _, _ = io.Copy(u, c); u.Close() })
		p.wg.Go(func() { p.copyDelayed(c, u); c.Close() })
	}
}

func (p *Proxy) copyDelayed(dst, src net.Conn) {
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if d := time.Duration(p.delay.Load()); d > 0 {
				time.Sleep(d)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
