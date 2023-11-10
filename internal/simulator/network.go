package simulator

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"math/rand"

	"github.com/tangledbytes/go-vsr/internal/simulator/array"
	"github.com/tangledbytes/go-vsr/internal/simulator/constant"
	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
	"github.com/tangledbytes/go-vsr/pkg/network"
	"github.com/tangledbytes/go-vsr/pkg/time"
	"github.com/tangledbytes/go-vsr/pkg/utils"
)

type Route struct {
	data *array.Rand[events.NetworkEvent]

	dropPercent float64
	dupsPercent float64
}

type Routes struct {
	rng        *rand.Rand
	idxSrcDest map[string]map[string]int
	idxSrcAny  map[string][]*Route

	logger *slog.Logger
}

func NewRoutes(rng *rand.Rand, logger *slog.Logger) *Routes {
	return &Routes{
		rng:        rng,
		idxSrcDest: make(map[string]map[string]int),
		idxSrcAny:  make(map[string][]*Route),

		logger: logger,
	}
}

func (r *Routes) AddPacket(src, dest string, ev events.NetworkEvent) bool {
	assert.Assert(ev.Event != nil, "event should not be nil")

	_, ok := r.idxSrcDest[dest]
	if !ok {
		r.idxSrcDest[dest] = make(map[string]int)
	}

	_, ok = r.idxSrcAny[dest]
	if !ok {
		r.idxSrcAny[dest] = make([]*Route, 0)
	}

	_, ok = r.idxSrcDest[dest][src]
	if !ok {
		r.idxSrcDest[dest][src] = len(r.idxSrcAny[dest])

		dupspercent := r.rng.Float64() * constant.PACKET_DUPS_PERCENT
		droppercent := r.rng.Float64() * constant.PACKET_DROP_PERCENT
		r.logger.Info(
			"new route created",
			"drop percent", droppercent*100,
			"dups percent", dupspercent*100,
		)

		r.idxSrcAny[dest] = append(r.idxSrcAny[dest], &Route{
			data:        array.NewRand[events.NetworkEvent](r.rng, r.logger),
			dropPercent: droppercent,
			dupsPercent: dupspercent,
		})
	}

	route := r.idxSrcAny[dest][r.idxSrcDest[dest][src]]
	if route.data.Len() >= constant.MAX_PACKET_INQUEUE {
		return false
	}

	dropChance := r.rng.Float64()
	if dropChance <= route.dropPercent {
		return true
	}

	route.data.Push(ev)
	return true
}

// SimulatedNetworkWorld is an awful name but can't think of anything better
type SimulatedNetworkWorld struct {
	routes *Routes

	time *time.Virtual
	rng  *rand.Rand

	logger *slog.Logger
}

func NewSimulatedNetworkWorld(time *time.Virtual, rng *rand.Rand, logger *slog.Logger) *SimulatedNetworkWorld {
	return &SimulatedNetworkWorld{
		routes: NewRoutes(rng, logger),
		time:   time,
		rng:    rng,

		logger: logger,
	}
}

// Node returns a network node for the given address, this can be called
// multiple times for the same address and it will return the same looking
// node.
func (snw *SimulatedNetworkWorld) Node(addr string) *SimulatedNetworkNode {
	ipv4port := ipv4port.IPv4Port{}
	if err := ipv4port.FromHostPort(addr); err != nil {
		panic(err)
	}

	return &SimulatedNetworkNode{
		routes: snw.routes,
		addr:   ipv4port,
		time:   snw.time,
		rng:    snw.rng,

		logger: snw.logger,
	}
}

type SimulatedNetworkNode struct {
	routes *Routes
	addr   ipv4port.IPv4Port
	time   *time.Virtual
	rng    *rand.Rand

	logger *slog.Logger
}

func (n *SimulatedNetworkNode) Send(dest ipv4port.IPv4Port, f func(io.Writer) error) error {
	buf := bytes.NewBuffer([]byte{})
	if err := f(buf); err != nil {
		return err
	}

	ev := &events.Event{}
	if err := ev.FromReader(buf); err != nil {
		return err
	}

	// add delay by ticking time
	delay := utils.RandomIntRange(n.rng, constant.MIN_PACKET_DELAY, constant.MAX_PACKET_DELAY)
	n.time.TickBy(uint64(delay))

	// duplications ?
	send := 1
	dupChance := n.rng.Float64()
	if dupChance <= constant.PACKET_DUPS_PERCENT {
		// Not doing for now - client isn't ready
		send = 1
	}

	for i := 0; i < send; i++ {
		if !n.routes.AddPacket(n.addr.String(), dest.String(), events.NewNetworkEvent(n.addr, ev)) {
			return fmt.Errorf("failed to add packet to route")
		}
	}

	return nil
}

func (n *SimulatedNetworkNode) Recv() (events.NetworkEvent, bool) {
	this := n.addr.String()

	l := len(n.routes.idxSrcAny[this])
	if l == 0 {
		return events.NetworkEvent{}, false
	}

	dest := n.rng.Intn(l)
	nev, ok := n.routes.idxSrcAny[this][dest].data.Pop()
	if !ok {
		return events.NetworkEvent{}, false
	}

	assert.Assert(nev.Event != nil, "event should not be nil")
	return nev, true
}

var _ network.Network = (*SimulatedNetworkNode)(nil)
