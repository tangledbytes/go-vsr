package simulator

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"

	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/client"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
	"github.com/tangledbytes/go-vsr/pkg/replica"
	"github.com/tangledbytes/go-vsr/pkg/time"
)

type Simulator struct {
	rng  *rand.Rand
	seed uint64

	net  *SimulatedNetworkWorld
	time *time.Virtual

	replicas      []*replica.Replica
	replicaCfgs   []replica.Config
	clients       []*client.Client
	clientCfgs    []client.Config
	replicaStores []*store

	simStore *store
}

type store struct {
	data map[string]string
}

func (s *store) Apply(m string) string {
	s.data[m] = m
	return m
}

func New(seed uint64) *Simulator {
	time := time.NewVirtual(time.MICROSECOND) // In this world, 1 mus is 1 tick.
	rng := rand.New(rand.NewSource(int64(seed)))

	return &Simulator{
		rng:  rng,
		seed: seed,

		net:  NewSimulatedNetworkWorld(time, rng),
		time: time,

		replicas:      make([]*replica.Replica, 0),
		replicaCfgs:   make([]replica.Config, 0),
		clients:       make([]*client.Client, 0),
		clientCfgs:    make([]client.Config, 0),
		replicaStores: make([]*store, 0),

		simStore: &store{data: make(map[string]string)},
	}
}

func (s *Simulator) Simulate() {
	replicaCount := 1 + s.rng.Intn(20) // Ensure at least 1 replica
	clientCount := 1 + s.rng.Intn(20)  // Ensure at least 1 client
	reqCount := 1e4 + s.rng.Intn(1e6)  // Ensure at least 1e4 requests

	slog.Info(
		"Simulation starting",
		"seed", s.seed,
		"replica_count", replicaCount,
		"client_count", clientCount,
		"request_count", reqCount,
	)

	shutuplogging()

	if err := s.initializeReplicaStores(replicaCount); err != nil {
		panic(err)
	}
	if err := s.initializeReplicas(clientCount); err != nil {
		panic(err)
	}
	if err := s.initializeClients(clientCount); err != nil {
		panic(err)
	}

	sentReq := 0
	processedReq := 0
	for {

		s.runReplicas()
		processedReq += s.runClients()
		sentReq += s.simulateRequests(sentReq, reqCount)

		s.time.Tick()

		// Done processing
		if processedReq == reqCount {
			break
		}
	}

	resumelogging()

	slog.Info(
		"Simulation complete",
		"ticks", s.time.Now(),
	)

	assert.Assert(sentReq == reqCount, "sentReq should be equal to reqCount")

	// Verify that cluster state is the same as our global state
	s.verifyClusterState(replicaCount)
}

func (s *Simulator) verifyClusterState(replicaCount int) {
	// quorum := replicaCount/2 + 1
	// invalids := 0
	// for k, v := range s.simStore.data {
	// 	found := 0
	// 	for _, store := range s.replicaStores {
	// 		if store.data[k] == v {
	// 			found++
	// 		}
	// 	}

	// 	if found < quorum {
	// 		println("found in less than quorum:", found)
	// 		invalids++
	// 	}
	// }

	// assert.Assert(invalids == 0, "expected invalids to be 0, found %d", invalids)

	// Ensure that more than half of the replicas have same opnum
	for i := 0; i < replicaCount; i++ {
	}
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
		for i := 0; i < s.rng.Intn(10); i++ {
			replica.Run()
		}
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
		for i := 0; i < s.rng.Intn(10); i++ {
			client.Run()
			_, ok = client.CheckResult()
			if ok {
				processedReq++
				println("processedReq:", processedReq)
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
	for i := 0; i < count; i++ {
		cfg := client.Config{
			ID:             uint64(i),
			Network:        s.net.Node(clientAddressByID(i)),
			Time:           s.time,
			Members:        s.replicaCfgs[0].Members,
			RequestTimeout: 30 * time.SECOND,
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

	for i := 0; i < count; i++ {
		cfg := replica.Config{
			ID:               uint64(i),
			Members:          members,
			Network:          s.net.Node(replicaAddressByID(i)),
			Time:             s.time,
			SrvHandler:       s.replicaStores[i].Apply,
			HeartbeatTimeout: 60 * time.SECOND,
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

func shutuplogging() {
	logh := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.Level(100),
	})
	slog.SetDefault(slog.New(logh))
}

func resumelogging() {
	logh := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{})
	slog.SetDefault(slog.New(logh))
}
