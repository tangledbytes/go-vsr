package client

import (
	"io"
	"log/slog"
	"sort"

	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
	"github.com/tangledbytes/go-vsr/pkg/network"
	"github.com/tangledbytes/go-vsr/pkg/queue"
	"github.com/tangledbytes/go-vsr/pkg/time"
)

type VSRState struct {
	// ID is the unique ID of the VSR client.
	ID uint64

	// RequestNumber is the monotonically increasing request
	// number of the client.
	RequestNumber uint64

	// LastKnownClusterMembers is the list of the last known
	// cluster members. This may not be the current cluster members.
	LastKnownClusterMembers []ipv4port.IPv4Port

	// LastKnownViewNumber is the last known view number of the cluster.
	LastKnownViewNumber uint64
}

type pendingRequest struct {
	reqTime uint64
	request *events.Request
	replied bool
}

type Internal struct {
	request  *queue.Queue[events.ClientRequest]
	response *queue.Queue[events.Reply]
	results  *queue.Queue[events.Reply]

	pendingRequest pendingRequest

	requestTimeout uint64
}

// Client represents a single client of the VSR cluster.
type Client struct {
	state    VSRState
	internal Internal
	net      network.Network
	time     time.Time
}

type Config struct {
	ID             uint64
	Members        []string
	Network        network.Network
	Time           time.Time
	RequestTimeout uint64
}

// New creates a new client.
func New(cfg Config) (*Client, error) {
	clustermembers := make([]ipv4port.IPv4Port, len(cfg.Members))
	for i, v := range cfg.Members {
		if err := clustermembers[i].FromHostPort(v); err != nil {
			return nil, err
		}
	}

	sort.Slice(clustermembers, func(i, j int) bool {
		return clustermembers[i].Less(clustermembers[j])
	})

	client := &Client{
		state: VSRState{
			ID:                      cfg.ID,
			RequestNumber:           0,
			LastKnownClusterMembers: clustermembers,
			LastKnownViewNumber:     0,
		},
		internal: Internal{
			request:        queue.New[events.ClientRequest](),
			response:       queue.New[events.Reply](),
			results:        queue.New[events.Reply](),
			requestTimeout: cfg.RequestTimeout,
		},
		net:  cfg.Network,
		time: cfg.Time,
	}

	return client, nil
}

func (c *Client) Submit(ev events.NetworkEvent) {
	switch ev.Event.Type {
	case events.EventClientRequest:
		c.internal.request.Push(ev.Event.Data.(events.ClientRequest))
	case events.EventReply:
		c.internal.response.Push(ev.Event.Data.(events.Reply))
	default:
		slog.Error("Received an invalid event", "event", ev)
	}
}

func (c *Client) CheckResult() (events.Reply, bool) {
	return c.internal.results.Pop()
}

func (c *Client) Run() {
	// Dequeue events from the internal queue if we do not have
	// have a pending request.
	if c.internal.pendingRequest.request == nil {
		ev, ok := c.internal.request.Pop()
		if ok {
			c.onRequest(ev)
		}
	}

	// Check if we have unprocessed replies.
	ev, ok := c.internal.response.Pop()
	if ok {
		c.onReply(ev)
	}

	// Check if it has been too long since we have received a reply.
	if c.internal.pendingRequest.request != nil && !c.internal.pendingRequest.replied {
		if c.time.Now()-c.internal.pendingRequest.reqTime > c.internal.requestTimeout {
			slog.Debug("client timed out waiting for a reply")

			// Requeue the request for broadcast
			c.onRequest(events.ClientRequest{
				Op:        c.internal.pendingRequest.request.Op,
				Broadcast: true,
			})
		}
	}
}

func (c *Client) onRequest(ev events.ClientRequest) {
	slog.Debug("client received request to run against the cluster", "request", ev)

	c.state.RequestNumber++

	if ev.Broadcast {
		for _, v := range c.state.LastKnownClusterMembers {
			if err := c.sendRequest(v, ev.Op); err != nil {
				slog.Error("client failed to send request to the cluster", "err", err)
			}
		}
	} else {
		if err := c.sendRequest(c.state.LastKnownClusterMembers[c.potentialPrimary()], ev.Op); err != nil {
			slog.Error("client failed to send request to the cluster", "err", err)
		}
	}
}

func (c *Client) onReply(ev events.Reply) {
	slog.Debug("client received reply from the cluster", "reply", ev)

	if c.internal.pendingRequest.request.ID == ev.ID {
		slog.Debug("client received reply for the pending request", "reply", ev)
		c.internal.pendingRequest.request = nil
		c.internal.pendingRequest.replied = true

		c.state.LastKnownViewNumber = ev.ViewNum
		c.internal.results.Push(ev)
	}
}

func (c *Client) sendRequest(to ipv4port.IPv4Port, op string) error {
	req := events.Request{
		ID:       c.state.RequestNumber,
		ClientID: c.state.ID,
		Op:       op,
	}

	c.internal.pendingRequest = pendingRequest{
		reqTime: c.time.Now(),
		request: &req,
	}

	return c.net.Send(to, func(w io.Writer) error {
		return (&events.Event{
			Type: events.EventRequest,
			Data: req,
		}).ToWriter(w)
	})
}

func (c *Client) potentialPrimary() uint64 {
	return uint64(int(c.state.LastKnownViewNumber) % len(c.state.LastKnownClusterMembers))
}
