package network

import (
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
)

type TCP struct {
	ip ipv4port.IPv4Port

	conns map[string]net.Conn

	ch chan events.NetworkEvent
	mu *sync.RWMutex
}

func NewTCP(hostname string) *TCP {
	ipv4port := ipv4port.IPv4Port{}
	err := ipv4port.FromHostPort(hostname)
	assert.Assert(err == nil, "err should be nil")

	return &TCP{
		ip:    ipv4port,
		conns: make(map[string]net.Conn),
		mu:    &sync.RWMutex{},
		ch:    make(chan events.NetworkEvent),
	}
}

func (tcp *TCP) Run() error {
	listener, err := net.Listen("tcp4", tcp.ip.String())
	if err != nil {
		return err
	}

	slog.Debug("started listener", "addr", tcp.ip.String())

	for {
		conn, err := listener.Accept()
		slog.Debug("accepted a connection", "remote-addr", conn.RemoteAddr().String())
		if err != nil {
			slog.Debug("failed to accept a connection", "err", err)
			continue
		}

		tcp.mu.Lock()
		tcp.conns[conn.RemoteAddr().String()] = conn
		tcp.mu.Unlock()

		go tcp.handleConn(conn)
	}
}

func (tcp *TCP) Send(ipv4port ipv4port.IPv4Port, f func(io.Writer) error) error {
	slog.Debug("received request to send", "ipv4port", ipv4port.String())

	tcp.mu.RLock()
	conn, ok := tcp.conns[ipv4port.String()]
	tcp.mu.RUnlock()
	if !ok {
		var err error
		conn, err = net.Dial("tcp4", ipv4port.String())
		if err != nil {
			slog.Debug("failed to dial", "err", err)
			return err
		}

		tcp.mu.Lock()
		tcp.conns[ipv4port.String()] = conn
		tcp.mu.Unlock()

		go tcp.handleConn(conn)
	}

	slog.Debug("attempting to send an event", "ipv4port", ipv4port.String())
	if err := f(conn); err != nil {
		slog.Debug("failed to send an event", "err", err)
		return err
	}
	slog.Debug("sent an event", "ipv4port", ipv4port.String())

	return nil
}

func (tcp *TCP) OnRecv(f func(events.NetworkEvent)) {
	go func() {
		slog.Debug("started the event handler")

		for pair := range tcp.ch {
			slog.Debug("received an event for handler", "ipv4port", pair.Src.String())
			f(pair)
			slog.Debug("completed event processing", "ipv4port", pair.Src.String())
		}

		slog.Debug("stopped the event receiver")
	}()
}

func (tcp *TCP) handleConn(conn net.Conn) {
	srcaddr := ipv4port.IPv4Port{}
	srcaddr.FromHostPort(conn.RemoteAddr().String())

	for {
		ev := &events.Event{}
		slog.Debug("conn waiting for an event")
		if err := ev.FromReader(conn); err != nil {
			slog.Debug("failed to read an event", "err", err)
			break
		}

		slog.Debug("conn received an event", "type", ev.Type, "from", srcaddr.String())
		tcp.ch <- events.NewNetworkEvent(srcaddr, ev)
		slog.Debug("conn sent an event to the handler", "type", ev.Type, "from", srcaddr.String())
	}

	tcp.mu.Lock()
	if err := conn.Close(); err != nil {
		slog.Debug("failed to properly close an errored connection", "err", err)
	}
	delete(tcp.conns, conn.RemoteAddr().String())
	tcp.mu.Unlock()
}

var _ Network = (*TCP)(nil)
