package simulator

import (
	"fmt"
	"log/slog"
	"math/rand"

	"github.com/tangledbytes/go-vsr/internal/simulator/constant"
	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/client"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
	"github.com/tangledbytes/go-vsr/pkg/replica"
	"github.com/tangledbytes/go-vsr/pkg/time"
	"github.com/tangledbytes/go-vsr/pkg/utils"
)

type Simulator struct {
	rng  *rand.Rand
	seed uint64

	net  *SimulatedNetworkWorld
	time *time.Virtual

	replicas           []*replica.Replica
	replicaCfgs        []replica.Config
	replicaMultipliers []int
	replicaStores      []*store

	clients           []*client.Client
	clientCfgs        []client.Config
	clientMultipliers []int

	replicaLogger *slog.Logger
	clientLogger  *slog.Logger
	logger        *slog.Logger

	simStore *store
}

type store struct {
	data map[string]string
}

func (s *store) Apply(m string) string {
	s.data[m] = m
	return m
}

func New(seed uint64, replicalogger, clientlogger, simlogger *slog.Logger) *Simulator {
	time := time.NewVirtual(100 * time.MICROSECOND) // In this world, 1 mus is 1 tick.
	rng := rand.New(rand.NewSource(int64(seed)))

	return &Simulator{
		rng:  rng,
		seed: seed,

		net:  NewSimulatedNetworkWorld(time, rng, simlogger),
		time: time,

		replicas:      make([]*replica.Replica, 0),
		replicaCfgs:   make([]replica.Config, 0),
		replicaStores: make([]*store, 0),

		clients:    make([]*client.Client, 0),
		clientCfgs: make([]client.Config, 0),

		replicaLogger: replicalogger,
		clientLogger:  clientlogger,
		logger:        simlogger,

		simStore: &store{data: make(map[string]string)},
	}
}

func (s *Simulator) Simulate() {
	replicaCount := utils.RandomIntRange(s.rng, constant.MIN_REPLICAS, constant.MAX_REPLICAS)
	clientCount := utils.RandomIntRange(s.rng, constant.MIN_CLIENTS, constant.MAX_CLIENTS)
	reqCount := utils.RandomIntRange(s.rng, constant.MIN_REQUESTS, constant.MAX_REQUESTS)

	s.logger.Info(
		"Simulation starting",
		"seed", s.seed,
		"replica_count", replicaCount,
		"client_count", clientCount,
		"request_count", reqCount,
	)

	if err := s.initializeReplicaStores(replicaCount); err != nil {
		panic(err)
	}
	if err := s.initializeReplicas(replicaCount); err != nil {
		panic(err)
	}
	if err := s.initializeClients(clientCount); err != nil {
		panic(err)
	}
	progressVerifier := s.clusterProgressVerifier()

	sentReq := 0
	processedReq := 0
	for {
		s.runReplicas()
		s.clusterSanityChecks()
		progressVerifier()
		sentReq += s.simulateRequests(sentReq, reqCount)
		processedReq += s.runClients()

		s.time.Tick()

		// Done processing
		if processedReq == reqCount {
			break
		}
	}

	s.logger.Info(
		"Simulation complete",
		"ticks", s.time.Now(),
	)

	assert.Assert(sentReq == reqCount, "sentReq should be equal to reqCount")

	// Verify that cluster state is the same as our global state
	s.verifyClusterVSRState()
}

func (s *Simulator) clusterProgressVerifier() func() {
	timer := time.NewTimer(s.time, 5*time.MINUTE)
	last := uint64(0)

	timer.Action(func(t *time.Timer) {
		opNum := findMaxOpNumber(s.replicas)
		if opNum == last {
			for _, replica := range s.replicas {
				state := replica.VSRState()
				s.logger.Info(
					"replica progress",
					"viewnum", state.ViewNumber,
					"opnum", state.OpNum,
					"commitnum", state.CommitNumber,
					"replicaID", state.ID,
				)
			}
		}
		assert.Assert(opNum > last, "expected opNum to be > %d (last), found %d", opNum, last)
		s.logger.Info("cluster progress:", "opnum", opNum)
		last = opNum

		t.Reset()
	})

	return func() {
		timer.ActIfDone()
	}
}

func (r *Simulator) clusterSanityChecks() {
	// 1. At no point any replica's opnum should be greater than its commitnum
	for _, replica := range r.replicas {
		state := replica.VSRState()
		assert.Assert(
			state.OpNum >= state.CommitNumber,
			"expected opNum to be <= commitNum, found opNum: %d, commitNum: %d",
			state.OpNum,
			state.CommitNumber,
		)
	}
}

func (s *Simulator) verifyClusterVSRState() {
	replicaCount := len(s.replicas)

	quorum := replicaCount/2 + 1
	vsrStates := make([]replica.VSRState, 0)
	for _, replica := range s.replicas {
		vsrStates = append(vsrStates, replica.VSRState())
	}

	viewMap := make(map[uint64]int)
	maxViews := 0
	opsMap := make(map[uint64]int)
	maxOps := 0
	for _, vsrState := range vsrStates {
		viewMap[vsrState.ViewNumber]++
		opsMap[vsrState.OpNum]++
	}

	// 1. Check if >= quorum of replicas have same view number
	for _, v := range viewMap {
		if v > maxViews {
			maxViews = v
		}
	}
	assert.Assert(maxViews >= quorum, "expected maxViews to be >= quorum, found %d", maxViews)
	// 2. Check if >= quorum of replicas have same op number
	for _, v := range opsMap {
		if v > maxOps {
			maxOps = v
		}
	}
	assert.Assert(maxOps >= quorum, "expected maxOps to be >= quorum, found %d", maxOps)
	// 3. Check if >= quorum of replicas have same commit number (?)
	// TODO
	// 4. Check if >= quorum of replicas have same log
	// TODO
	// 5. Check if >= quorum of replicas have same store (?)
	invalids := 0
	for k, v := range s.simStore.data {
		found := 0
		for _, store := range s.replicaStores {
			if store.data[k] == v {
				found++
			}
		}

		if found < quorum {
			invalids++
		}
	}

	// simulation might end before last commit so we check for <= 1
	assert.Assert(invalids <= 1, "expected invalids to be <=1 , found %d", invalids)
}

func (s *Simulator) runReplicas() {
	for i, replica := range s.replicas {
		cfg := s.replicaCfgs[i]
		ev, ok := cfg.Network.Recv()
		if ok {
			replica.Submit(ev)
		}

		// Run the replica a random number of times.
		// Hopefully simulates powerful hardware for
		// some replicas while weaker for others?
		for j := 0; j < s.replicaMultipliers[i]; j++ {
			replica.Run()
		}

		state := replica.VSRState()
		s.logger.Debug(
			"VSR State",
			"viewnum", state.ViewNumber,
			"opnum", state.OpNum,
			"commitnum", state.CommitNumber,
			"replicaID", state.ID,
		)
	}
}

func (s *Simulator) runClients() int {
	processedReq := 0

	for i, client := range s.clients {
		cfg := s.clientCfgs[i]
		ev, ok := cfg.Network.Recv()
		if ok {
			client.Submit(ev)
		}

		// Run the clients a random number of times.
		// Hopefully simulates more aggresseive clients
		for j := 0; j < s.clientMultipliers[i]; j++ {
			client.Run()
			_, ok = client.CheckResult()
			if ok {
				processedReq++
				s.logger.Debug("client processed request", "processed", processedReq)
			}
		}
	}

	return processedReq
}

func (s *Simulator) simulateRequests(sentReq, reqCount int) int {
	if sentReq < reqCount {
		reqbatch := s.rng.Intn(100)
		if reqbatch > reqCount-sentReq {
			reqbatch = reqCount - sentReq
		}

		for i := 0; i < reqbatch; i++ {
			cID := s.rng.Intn(len(s.clients))
			client := s.clients[cID]

			key := fmt.Sprintf("entry-%d", s.rng.Int())
			s.simStore.Apply(key)

			sendRequest(client, key)
		}

		return reqbatch
	}

	return 0
}

func (s *Simulator) initializeClients(count int) error {
	if err := s.initializeClientConfigs(count); err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		client, err := client.New(s.clientCfgs[i])
		if err != nil {
			return err
		}

		s.clients = append(s.clients, client)
	}

	return nil
}

func (s *Simulator) initializeClientConfigs(count int) error {
	s.clientMultipliers = make([]int, count)

	for i := 0; i < count; i++ {
		s.clientMultipliers[i] = utils.RandomIntRange(s.rng, 1, constant.MAX_CLIENT_MULTIPLIER+1)

		cfg := client.Config{
			ID:             uint64(i),
			Network:        s.net.Node(clientAddressByID(i)),
			Time:           s.time,
			Members:        s.replicaCfgs[0].Members,
			RequestTimeout: 30 * time.SECOND,
			Logger:         s.clientLogger,
		}

		s.clientCfgs = append(s.clientCfgs, cfg)
	}

	return nil
}

func (s *Simulator) initializeReplicas(count int) error {
	if err := s.initializeReplicaConfigs(count); err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		replica, err := replica.New(s.replicaCfgs[i])
		if err != nil {
			return err
		}

		s.replicas = append(s.replicas, replica)
	}

	return nil
}

func (s *Simulator) initializeReplicaConfigs(count int) error {
	members := []string{}
	for i := 0; i < count; i++ {
		members = append(members, replicaAddressByID(i))
	}

	if err := s.initializeReplicaStores(count); err != nil {
		return err
	}

	s.replicaMultipliers = make([]int, count)

	for i := 0; i < count; i++ {
		s.replicaMultipliers[i] = utils.RandomIntRange(s.rng, 1, constant.MAX_REPLICA_MULTIPLIER+1)

		cfg := replica.Config{
			ID:               uint64(i),
			Members:          members,
			Network:          s.net.Node(replicaAddressByID(i)),
			Time:             s.time,
			SrvHandler:       s.replicaStores[i].Apply,
			HeartbeatTimeout: 60 * time.SECOND,
			Logger:           s.replicaLogger,
		}
		s.replicaCfgs = append(s.replicaCfgs, cfg)
	}

	return nil
}

func (s *Simulator) initializeReplicaStores(count int) error {
	for i := 0; i < count; i++ {
		s.replicaStores = append(s.replicaStores, &store{data: make(map[string]string)})
	}

	return nil
}

func sendRequest(c *client.Client, data string) {
	c.Submit(events.NewNetworkEvent(
		ipv4port.IPv4Port{},
		&events.Event{
			Type: events.EventClientRequest,
			Data: events.ClientRequest{
				Op: data,
			},
		},
	))
}

func replicaAddressByID(id int) string {
	return fmt.Sprintf("0.0.0.0:%d", 10000+id)
}

func clientAddressByID(id int) string {
	return fmt.Sprintf("0.0.0.0:%d", 20000+id)
}

func findMaxOpNumber(replicas []*replica.Replica) uint64 {
	max := uint64(0)
	for _, replica := range replicas {
		state := replica.VSRState()
		if state.OpNum > max {
			max = state.OpNum
		}
	}

	return max
}
