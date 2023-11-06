package network

import (
	"io"

	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
)

type Network interface {
	Send(ipv4port.IPv4Port, func(io.Writer) error) error
	OnRecv(func(events.NetworkEvent))
}
