package network

import (
	"bytes"
	"io"

	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
)

type Simulated struct {
	routes map[string]func(events.NetworkEvent)
}

func NewSimulated() *Simulated {
	return &Simulated{
		routes: make(map[string]func(events.NetworkEvent)),
	}
}

func (n *Simulated) Send(dest ipv4port.IPv4Port, f func(io.Writer) error) error {
	handler, ok := n.routes[dest.String()]
	assert.Assert(ok, "route should exist")

	buf := bytes.NewBuffer([]byte{})
	if err := f(buf); err != nil {
		return err
	}

	ev := &events.Event{}
	if err := ev.FromReader(buf); err != nil {
		return err
	}

	handler(events.NewNetworkEvent(dest, ev))
	return nil
}

func (n *Simulated) AddRoute(ipv4port string, f func(events.NetworkEvent)) {
	n.routes[ipv4port] = f
}

func (n *Simulated) OnRecv(f func(events.NetworkEvent)) {
	// do nothing
}

var _ Network = (*Simulated)(nil)
