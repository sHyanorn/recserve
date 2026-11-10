// Package valkeyclient keeps connections to one Valkey server open in the
// background, so that no call opens one within its own context
// (docs/DECISIONS.md D037). valkey-go opens a connection, handshake
// included, under the context of the call that first needs it; here every
// connection is a valkey-go client that may connect once, made and replaced
// by one goroutine, and a call only picks among those that are connected.
package valkeyclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

const (
	// conns is the number of connections kept, valkey-go's own for one
	// server on four or more CPUs.
	conns = 4
	// dialTimeout bounds one attempt to connect, handshake included.
	dialTimeout = time.Second
	// retryEvery is the wait after a failed attempt.
	retryEvery = 100 * time.Millisecond
)

var (
	// ErrConnecting is the error of a call whose context ended while the
	// client had no connection and the first attempt to open one was still
	// running. It says nothing about the server yet.
	ErrConnecting = errors.New("valkey: no connection yet")
	// ErrClosed is the error of a call on a closed client.
	ErrClosed = errors.New("valkey: client closed")

	errSecondDial = errors.New("valkey: connection lost")
)

// Client is a set of connections to one server.
type Client struct {
	addr string

	state atomic.Pointer[state]
	mu    sync.Mutex // held to change state, lost and closed
	lost  []*conn    // taken out of the state, for the keeper to close
	// closed is set by Close; the state then stays as Close left it.
	closed bool

	kick chan struct{} // tells the keeper that a connection is missing
	done chan struct{}
	wg   sync.WaitGroup
}

// state is what a call sees; a change replaces it and closes next.
type state struct {
	up []*conn
	// err, with no connection up, is why: the last attempt's error, or
	// ErrClosed. It is nil until an attempt begun with no connection up
	// has failed.
	err  error
	next chan struct{}
}

type conn struct {
	v    valkey.Client
	dead atomic.Bool // its network connection was closed
}

// New connects to the server at addr and fails if it cannot; further
// connections are opened in the background.
func New(addr string) (*Client, error) {
	c := &Client{addr: addr, kick: make(chan struct{}, 1), done: make(chan struct{})}
	first, err := c.connect()
	if err != nil {
		return nil, fmt.Errorf("valkey %s: %w", addr, err)
	}
	c.state.Store(&state{up: []*conn{first}, next: make(chan struct{})})
	c.wg.Go(c.keep)
	return c, nil
}

// connect opens one connection, at most dialTimeout long. The valkey-go
// client it returns holds that connection only: a second dial fails at
// once, so a call on it never waits for a connection to open, and the close
// of the network connection takes it out of the state.
func (c *Client) connect() (*conn, error) {
	cn := &conn{}
	var dialed atomic.Bool
	v, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{c.addr},
		ForceSingleClient: true, // one server: no cluster probe
		DisableCache:      true, // no client-side caching, so no CLIENT TRACKING
		DisableRetry:      true, // the client would retry a read until the context ends
		PipelineMultiplex: -1,   // one connection
		Dialer:            net.Dialer{Timeout: dialTimeout},
		DialCtxFn: func(ctx context.Context, dst string, d *net.Dialer, _ *tls.Config) (net.Conn, error) {
			if dialed.Swap(true) {
				return nil, errSecondDial
			}
			nc, err := d.DialContext(ctx, "tcp", dst)
			if err != nil {
				return nil, err
			}
			return notifyConn{nc, func() { c.drop(cn) }}, nil
		},
	})
	if err != nil {
		if v != nil { // with ForceSingleClient a client comes back even so
			v.Close()
		}
		return nil, err
	}
	cn.v = v
	return cn, nil
}

// notifyConn reports its close, which is how valkey-go ends a connection
// that failed.
type notifyConn struct {
	net.Conn
	closed func()
}

func (c notifyConn) Close() error {
	c.closed()
	return c.Conn.Close()
}

// Do runs call on a connection. If that connection is lost meanwhile and
// ctx has not ended, call runs again on another, so what it sends must be
// safe to send twice. With no connection, Do waits for one as long as ctx
// allows and then fails with ErrConnecting; once an attempt to connect has
// failed, it fails at once with that attempt's error until one succeeds.
func (c *Client) Do(ctx context.Context, call func(valkey.Client) error) error {
	for {
		st := c.state.Load()
		if len(st.up) == 0 {
			if st.err != nil {
				return st.err
			}
			select {
			case <-st.next:
				continue
			case <-ctx.Done():
				return fmt.Errorf("%w: %w", ErrConnecting, ctx.Err())
			}
		}
		cn := st.up[rand.IntN(len(st.up))]
		if err := call(cn.v); err == nil || !cn.dead.Load() || ctx.Err() != nil {
			return err
		}
	}
}

// Close closes the connections; calls then fail with ErrClosed.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	open := slices.Concat(c.state.Load().up, c.lost)
	c.lost = nil
	c.publish(&state{err: ErrClosed})
	c.mu.Unlock()
	close(c.done)
	c.wg.Wait()
	for _, cn := range open {
		cn.v.Close()
	}
}

// publish replaces the state; the caller holds mu.
func (c *Client) publish(st *state) {
	st.next = make(chan struct{})
	close(c.state.Swap(st).next)
}

// drop takes a connection whose network connection was closed out of the
// state and wakes the keeper. valkey-go calls it while it closes the
// connection, so it must not call back into cn.v.
func (c *Client) drop(cn *conn) {
	if cn.dead.Swap(true) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state.Load()
	i := slices.Index(st.up, cn)
	if i < 0 { // not added yet, or the client is closed
		return
	}
	c.lost = append(c.lost, cn)
	c.publish(&state{up: slices.Delete(slices.Clone(st.up), i, i+1)})
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// keep closes the connections dropped and opens new ones in their place.
func (c *Client) keep() {
	for {
		c.mu.Lock()
		lost, missing := c.lost, conns-len(c.state.Load().up)
		c.lost = nil
		c.mu.Unlock()
		for _, cn := range lost {
			cn.v.Close()
		}
		if missing == 0 {
			select {
			case <-c.kick:
				continue
			case <-c.done:
				return
			}
		}
		select {
		case <-c.done:
			return
		default:
		}
		// With no connection up, calls wait for this attempt. One begun
		// while some were up does not count for them: it may have failed
		// with those.
		first := missing == conns
		cn, err := c.connect()
		if !c.add(cn, err, first) && cn != nil {
			cn.v.Close()
		}
		if err != nil {
			select {
			case <-time.After(retryEvery):
			case <-c.done:
				return
			}
		}
	}
}

// add publishes the outcome of an attempt to connect and reports whether
// the state now holds cn. A failure is published only with no connection
// up, and only if none was up when the attempt began (first).
func (c *Client) add(cn *conn, err error, first bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state.Load()
	switch {
	case c.closed:
		return false
	case err != nil:
		if first && len(st.up) == 0 {
			c.publish(&state{err: err})
		}
		return false
	case cn.dead.Load():
		return false
	}
	c.publish(&state{up: append(slices.Clone(st.up), cn)})
	return true
}
