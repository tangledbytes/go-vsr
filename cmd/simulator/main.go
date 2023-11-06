package main

import (
	"fmt"

	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/network"
	"github.com/tangledbytes/go-vsr/pkg/replica"
	"github.com/tangledbytes/go-vsr/pkg/time"
)

func generateMembers(num int) []string {
	members := make([]string, num)
	for i := 0; i < num; i++ {
		members[i] = fmt.Sprintf("0.0.0.0:1000%d", i)
	}

	return members
}

func generateReplicas(tolerance int, network *network.Simulated, time *time.Simulated) []*replica.Replica {
	replicas := make([]*replica.Replica, tolerance*2+1)
	members := generateMembers(tolerance*2 + 1)
	for i := 0; i < tolerance*2+1; i++ {
		cfg := replica.Config{
			ID:      i,
			Members: members,
			Network: network,
			Time:    time,
			SrvHandler: func(m string) string {
				return "{}"
			},
			HeartbeatTimeout: 15,
		}
		replica, err := replica.New(cfg)

		assert.Assert(err == nil, "err should be nil")
		replicas[i] = replica

		network.AddRoute(members[i], func(ev events.NetworkEvent) {
			replica.Submit(ev)
		})
	}

	return replicas
}

func runReplica(r *replica.Replica, time time.Time, n int) {
	for i := 0; i < n; i++ {
		r.Run()
		time.Tick()
	}
}

func main() {
	fmt.Println("Simulator starting...")

	net := network.NewSimulated()
	time := time.NewSimulated(1)
	replicas := generateReplicas(1, net, time)

	for {
		for _, r := range replicas {
			runReplica(r, time, 1)
		}
	}
}
