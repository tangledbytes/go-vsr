package simulator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"

	"github.com/tangledbytes/go-vsr/internal/array"
	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
	"github.com/tangledbytes/go-vsr/pkg/network"
	"github.com/tangledbytes/go-vsr/pkg/time"
)

type NetworkRing = array.Rand[events.NetworkEvent]

type Routes struct {
	rng         *rand.Rand
	dropPercent float64
	idxSrcDest  map[string]map[string]int
	idxSrcAny   map[string][]*NetworkRing
}

func NewRoutes(rng *rand.Rand) *Routes {
	droppercent := rng.Float64() * 0.5
	slog.Log(
		context.Background(),
		slog.Level(108),
		"new route created",
		"drop percent", droppercent,
	)

	return &Routes{
		rng: rng,
		// Can simulate partition as well, not sure if that's good
		dropPercent: droppercent,
		idxSrcDest:  make(map[string]map[string]int),
		idxSrcAny:   make(map[string][]*NetworkRing),
	}
}

func (r *Routes) AddPacket(src, dest string, ev events.NetworkEvent) bool {
	assert.Assert(ev.Event != nil, "event should not be nil")
	// println("add packet", src, dest)

	_, ok := r.idxSrcDest[dest]
	if !ok {
		r.idxSrcDest[dest] = make(map[string]int)
	}

	_, ok = r.idxSrcAny[dest]
	if !ok {
		r.idxSrcAny[dest] = make([]*NetworkRing, 0)
	}

	_, ok = r.idxSrcDest[dest][src]
	if !ok {
		r.idxSrcDest[dest][src] = len(r.idxSrcAny[dest])
		r.idxSrcAny[dest] = append(r.idxSrcAny[dest], array.NewRand[events.NetworkEvent](r.rng))
	}

	if r.idxSrcAny[dest][r.idxSrcDest[dest][src]].Len() > 4096 {
		return false
	}

	dropChance := r.rng.Float64()
	if dropChance <= r.dropPercent {
		return true
	}

	r.idxSrcAny[dest][r.idxSrcDest[dest][src]].Push(ev)
	return true
}

// SimulatedNetworkWorld is an awful name but can't think of anything better
type SimulatedNetworkWorld struct {
	routes *Routes

	time *time.Virtual
	rng  *rand.Rand
}

func NewSimulatedNetworkWorld(time *time.Virtual, rng *rand.Rand) *SimulatedNetworkWorld {
	return &SimulatedNetworkWorld{
		routes: NewRoutes(rng),
		time:   time,
		rng:    rng,
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
	}
}

type SimulatedNetworkNode struct {
	routes *Routes
	addr   ipv4port.IPv4Port
	time   *time.Virtual
	rng    *rand.Rand
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
	delay := time.MILLISECOND + n.rng.Intn(2*time.MINUTE)
	n.time.TickBy(uint64(delay))

	// duplications ?

	if !n.routes.AddPacket(n.addr.String(), dest.String(), events.NewNetworkEvent(n.addr, ev)) {
		return fmt.Errorf("failed to add packet to route")
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
	nev, ok := n.routes.idxSrcAny[this][dest].Pop()
	if !ok {
		return events.NetworkEvent{}, false
	}

	assert.Assert(nev.Event != nil, "event should not be nil")
	return nev, true
}

var _ network.Network = (*SimulatedNetworkNode)(nil)
