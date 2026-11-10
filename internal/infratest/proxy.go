package infratest

import (
	"io"
	"net"
	"sync"
	"testing"
)

// Proxy is a TCP proxy in front of a server, for tests that take the
// server away: it passes connections on, refuses them, or accepts them and
// never answers.
type Proxy struct {
	t      testing.TB
	target string
	addr   string

	mu     sync.Mutex
	ln     net.Listener // nil while refusing
	silent bool
	conns  map[net.Conn]bool // true for a client's side
}

// NewProxy returns a proxy on a local port that passes connections on to
// target. It is closed when the test ends.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	p := &Proxy{t: t, target: target, addr: "127.0.0.1:0", conns: map[net.Conn]bool{}}
	p.Pass()
	p.addr = p.ln.Addr().String()
	t.Cleanup(p.Refuse)
	return p
}

// Addr returns the address clients connect to.
func (p *Proxy) Addr() string { return p.addr }

// Pass passes connections on to the server. Connections left without an
// answer are closed, so their clients connect again.
func (p *Proxy) Pass() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.silent = false
	p.closeConns(true)
	p.listen()
}

// Refuse closes every connection and refuses new ones.
func (p *Proxy) Refuse() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		if err := p.ln.Close(); err != nil {
			p.t.Error(err)
		}
		p.ln = nil
	}
	p.closeConns(true)
}

// Blackhole stops answering: connections stay open, new ones are accepted,
// and nothing a client sends reaches the server or gets a reply.
func (p *Proxy) Blackhole() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.silent = true
	p.closeConns(false)
	p.listen()
}

// closeConns closes the connections to the server and, if clients, those
// from the clients.
func (p *Proxy) closeConns(clients bool) {
	for c, client := range p.conns {
		if clients || !client {
			c.Close() //nolint:errcheck // its peer may have closed it already
			delete(p.conns, c)
		}
	}
}

func (p *Proxy) listen() {
	if p.ln != nil {
		return
	}
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		p.t.Fatal(err)
	}
	p.ln = ln
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(ln, c)
		}
	}()
}

func (p *Proxy) serve(ln net.Listener, c net.Conn) {
	p.mu.Lock()
	if p.ln != ln { // refused since it was accepted
		p.mu.Unlock()
		c.Close() //nolint:errcheck // nothing was sent on it
		return
	}
	p.conns[c] = true
	silent := p.silent
	p.mu.Unlock()
	if silent {
		return
	}
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		p.t.Error(err)
		return
	}
	p.mu.Lock()
	p.conns[up] = false
	p.mu.Unlock()
	go func() {
		io.Copy(up, c) //nolint:errcheck // ends when either side closes
		up.Close()     //nolint:errcheck // the server's side may be closed already
	}()
	io.Copy(c, up) //nolint:errcheck // ends when either side closes
	// The server's side ended. Unless the proxy went silent, in which case
	// the client's side stays open and unanswered, the client's ends too.
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.conns, up)
	up.Close() //nolint:errcheck // closed already if the proxy took the server away
	if !p.silent && p.conns[c] {
		delete(p.conns, c)
		c.Close() //nolint:errcheck // the client may have closed it already
	}
}
