package valkeyclient

import "github.com/valkey-io/valkey-go"

// NewDialingInCalls returns a client that is valkey-go's own with the
// options the gateway gave it before this package: it connects once at the
// start and opens every other connection within the call that first uses
// it.
func NewDialingInCalls(addr string) (*Client, error) {
	v, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		DisableCache:      true,
		DisableRetry:      true,
	})
	if err != nil {
		if v != nil {
			v.Close()
		}
		return nil, err
	}
	c := &Client{addr: addr, kick: make(chan struct{}, 1), done: make(chan struct{})}
	c.state.Store(&state{up: []*conn{{v: v}}, next: make(chan struct{})})
	return c, nil
}
