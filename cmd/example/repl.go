package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/client"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
	"github.com/tangledbytes/go-vsr/pkg/network"
	"github.com/tangledbytes/go-vsr/pkg/time"
)

type repl struct {
	clientid       int
	clustermembers []string

	time *time.Real
	net  *network.TCP
}

func newRepl(members []string, id int) *repl {
	net := network.NewTCP("0.0.0.0:0")
	t := time.NewReal()

	return &repl{
		clustermembers: members,
		time:           t,
		net:            net,
		clientid:       id,
	}
}

func (r *repl) run() {
	client, err := client.New(client.Config{
		ID:             uint64(r.clientid),
		Members:        r.clustermembers,
		Network:        r.net,
		Time:           r.time,
		RequestTimeout: 10 * time.SECOND,
	})
	assert.Assert(err == nil, "err should be nil")

	go r.net.Run()

	waiting := false
	for {
		if !waiting {
			cmd, ok := r.acceptinput()
			if !ok {
				fmt.Println("Error reading input")
				continue
			}

			client.Submit(events.NewNetworkEvent(ipv4port.IPv4Port{}, r.eventFromCmd(cmd)))
			waiting = true
		}

		ev, ok := r.net.Recv()
		if ok {
			client.Submit(ev)
		}

		client.Run()
		r.time.Tick()

		if waiting {
			reply, ok := client.CheckResult()
			if !ok {
				continue
			}

			fmt.Println("server=>", reply.Result)
			waiting = false
		}
	}
}

func (r *repl) acceptinput() (string, bool) {
	fmt.Print("client=>")
	buf := bufio.NewReader(os.Stdin)
	cmd, err := buf.ReadString('\n')
	return strings.TrimSpace(cmd), err == nil
}

func (r *repl) eventFromCmd(cmd string) *events.Event {
	return &events.Event{
		Type: events.EventClientRequest,
		Data: events.ClientRequest{
			Op: cmd,
		},
	}
}
