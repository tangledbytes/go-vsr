package main

import (
	"fmt"
	"strings"

	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/network"
	"github.com/tangledbytes/go-vsr/pkg/replica"
	"github.com/tangledbytes/go-vsr/pkg/time"
)

type server struct {
	members []string
	id      int
	net     *network.TCP
	time    *time.Real

	store map[string]string
}

func newServer(members []string, port string, id int) *server {
	net := network.NewTCP("0.0.0.0:" + port)
	return &server{
		members: members,
		id:      id,
		net:     net,
		store:   make(map[string]string),
	}
}

func (s *server) run() {
	replica, err := replica.New(replica.Config{
		ID:               s.id,
		Members:          s.members,
		Network:          s.net,
		SrvHandler:       s.handleCmd,
		Time:             s.time,
		HeartbeatTimeout: 30 * time.SECOND,
	})

	assert.Assert(err == nil, "err should be nil")

	s.net.OnRecv(func(ne events.NetworkEvent) {
		replica.Submit(ne)
	})
	go s.net.Run()

	for {
		replica.Run()
		s.time.Tick()
	}
}

func (s *server) handleCmd(m string) string {
	cmd := strings.Split(m, " ")
	if len(cmd) < 2 || len(cmd) > 3 {
		return "{}"
	}

	if len(cmd) == 2 {
		if cmd[0] != "GET" {
			return "{}"
		}

		return fmt.Sprintf("{\"result\": \"%s\"}", s.store[cmd[1]])
	}

	if len(cmd) == 3 {
		if cmd[0] != "SET" {
			return "{}"
		}

		s.store[cmd[1]] = cmd[2]
		return fmt.Sprintf("{\"result\": \"%s\"}", cmd[2])
	}

	return "{}"
}
