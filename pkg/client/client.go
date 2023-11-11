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

type Internal struct {
	response *queue.Queue[events.Reply]

	request        *events.Request
	result         *events.Reply
	requestTimeout uint64
	requestTimer   *time.Timer
}

// Client represents a single client of the VSR cluster.
type Client struct {
	state    VSRState
	internal Internal

	net  network.Network
	time time.Time

	logger *slog.Logger
}

type Config struct {
	ID             uint64
	Members        []string
	Network        network.Network
	Time           time.Time
	RequestTimeout uint64
	Logger         *slog.Logger
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
			response:       queue.New[events.Reply](),
			requestTimeout: cfg.RequestTimeout,
		},
		net:    cfg.Network,
		time:   cfg.Time,
		logger: cfg.Logger,
	}

	return client, nil
}

func (c *Client) Submit(ev events.NetworkEvent) {
	switch ev.Event.Type {
	case events.EventReply:
		c.internal.response.Push(ev.Event.Data.(events.Reply))
	default:
		c.logger.Error("Received an invalid event", "event", ev)
	}
}

func (c *Client) Request(op string) bool {
	if c.internal.request != nil {
		return false
	}

	c.state.RequestNumber++
	clusterRequest := &events.Request{
		ID:       c.state.RequestNumber,
		ClientID: c.state.ID,
		Op:       op,
	}

	c.internal.request = clusterRequest

	if err := c.sendRequest(c.state.LastKnownClusterMembers[c.potentialPrimary()], op); err != nil {
		return false
	}

	c.internal.requestTimer = time.NewTimer(c.time, c.internal.requestTimeout)
	return true
}

func (c *Client) CheckResult() (events.Reply, bool) {
	if c.internal.result == nil {
		return events.Reply{}, false
	}

	resp := events.Reply{
		ID:       c.internal.result.ID,
		ClientID: c.internal.result.ClientID,
		ViewNum:  c.internal.result.ViewNum,
		Result:   c.internal.result.Result,
	}

	c.internal.result = nil
	return resp, true
}

func (c *Client) Run() {
	if c.internal.request != nil {
		if c.internal.requestTimer.Done() {
			c.logger.Debug("client timed out waiting for a reply")

			// Request for broadcast
			if err := c.broadcastRequest(c.internal.request.Op); err != nil {
				c.logger.Error("client failed to broadcast request", "error", err)
			}

			// Reset the timer but don't give up the request slot yet
			// next run will check the timer and will attempt the
			// broadcast again.
			c.internal.requestTimer.Reset()
		}
	}

	// Check if we have unprocessed replies.
	ev, ok := c.internal.response.Pop()
	if ok {
		c.onReply(ev)
	}
}

func (c *Client) onReply(ev events.Reply) {
	c.logger.Debug("client received reply from the cluster", "reply", ev)

	// No requests were pending, why am I here? Probably a duplicate or
	// delayed reply.
	if c.internal.request == nil {
		c.logger.Debug("client received a reply but no request was pending", "reply", ev)
		return
	}

	if c.internal.request.ID == ev.ID {
		c.logger.Debug("client received reply for the pending request", "reply", ev)
		c.internal.request = nil
		c.internal.result = &ev
		c.state.LastKnownViewNumber = ev.ViewNum
		c.internal.requestTimer = nil
	}
}

func (c *Client) sendRequest(to ipv4port.IPv4Port, op string) error {
	req := events.Request{
		ID:       c.state.RequestNumber,
		ClientID: c.state.ID,
		Op:       op,
	}

	return c.net.Send(to, func(w io.Writer) error {
		return (&events.Event{
			Type: events.EventRequest,
			Data: req,
		}).ToWriter(w)
	})
}

func (c *Client) broadcastRequest(op string) error {
	for _, member := range c.state.LastKnownClusterMembers {
		if err := c.sendRequest(member, op); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) potentialPrimary() uint64 {
	return uint64(int(c.state.LastKnownViewNumber) % len(c.state.LastKnownClusterMembers))
}
