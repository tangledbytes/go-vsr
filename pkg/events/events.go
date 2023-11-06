package events

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
	"github.com/tangledbytes/go-vsr/pkg/log"
)

type EventType uint8

const (
	EventRequest EventType = iota + 1
	EventReply
	EventPrepare
	EventPrepareOK
	EventCommit
	EventStartViewChange
	EventDoViewChange
	EventStartView
	EventHeartbeat

	EventClientRequest
)

// ErrInvalidEventType is returned when an invalid event type is encountered.
var ErrInvalidEventType = errors.New("invalid event type")

type NetworkEvent struct {
	Src   ipv4port.IPv4Port
	Event *Event
}

type Event struct {
	Type EventType `json:"@type"`
	Data any       `json:"data"`
}

type Request struct {
	ID       uint64 `json:"id,string"`
	ClientID uint64 `json:"client_id,string"`
	Op       string `json:"op"`
}

type Reply struct {
	ID       uint64 `json:"id,string"`
	ClientID uint64 `json:"client_id,string"`
	ViewNum  uint64 `json:"view_num,string"`
	Result   string `json:"result"`
}

type Prepare struct {
	ViewNum   uint64  `json:"view_num,string"`
	OpNum     uint64  `json:"op_num,string"`
	CommitNum uint64  `json:"commit_num,string"`
	Request   Request `json:"request"`
	ReplicaID uint64  `json:"replica_id,string"`
}

type PrepareOK struct {
	ViewNum   uint64 `json:"view_num,string"`
	OpNum     uint64 `json:"op_num,string"`
	ReplicaID uint64 `json:"replica_id,string"`
}

type Commit struct {
	ViewNum   uint64 `json:"view_num,string"`
	CommitNum uint64 `json:"commit_num,string"`
	ReplicaID uint64 `json:"replica_id,string"`
}

type StartViewChange struct {
	ViewNum   uint64 `json:"view_num,string"`
	ReplicaID uint64 `json:"replica_id,string"`
}

type DoViewChange struct {
	ViewNum           uint64   `json:"view_num,string"`
	Logs              log.Logs `json:"logs"`
	LastStableViewNum uint64   `json:"last_stable_view_num,string"`
	OpNum             uint64   `json:"op_num,string"`
	CommitNum         uint64   `json:"commit_num,string"`
	ReplicaID         uint64   `json:"replica_id,string"`
}

type StartView struct {
	ViewNum   uint64   `json:"view_num,string"`
	OpNum     uint64   `json:"op_num,string"`
	CommitNum uint64   `json:"commit_num,string"`
	Logs      log.Logs `json:"logs"`
	ReplicaID uint64   `json:"replica_id,string"`
}

type Heartbeat struct{}

type ClientRequest struct {
	Op        string `json:"op"`
	Broadcast bool   `json:"broadcast"`
}

func (ev *Event) FromReader(r io.Reader) error {
	temp := struct {
		Type EventType       `json:"@type"`
		Data json.RawMessage `json:"data"`
	}{}

	if err := json.NewDecoder(r).Decode(&temp); err != nil {
		return err
	}

	ev.Type = temp.Type
	var err error

	switch ev.Type {
	case EventRequest:
		ev.Data, err = unmarshal[Request](temp.Data)
	case EventPrepare:
		ev.Data, err = unmarshal[Prepare](temp.Data)
	case EventPrepareOK:
		ev.Data, err = unmarshal[PrepareOK](temp.Data)
	case EventCommit:
		ev.Data, err = unmarshal[Commit](temp.Data)
	case EventStartViewChange:
		ev.Data, err = unmarshal[StartViewChange](temp.Data)
	case EventDoViewChange:
		ev.Data, err = unmarshal[DoViewChange](temp.Data)
	case EventStartView:
		ev.Data, err = unmarshal[StartView](temp.Data)
	case EventHeartbeat:
		ev.Data, err = unmarshal[Heartbeat](temp.Data)
	case EventReply:
		ev.Data, err = unmarshal[Reply](temp.Data)
	case EventClientRequest:
		ev.Data, err = unmarshal[ClientRequest](temp.Data)
	default:
		return ErrInvalidEventType
	}

	if err != nil {
		return err
	}

	return nil
}

func (ev *Event) ToWriter(w io.Writer) error {
	if err := json.NewEncoder(w).Encode(ev); err != nil {
		return err
	}

	return nil
}

func NewNetworkEvent(src ipv4port.IPv4Port, ev *Event) NetworkEvent {
	return NetworkEvent{
		Src:   src,
		Event: ev,
	}
}

func unmarshal[T any](data json.RawMessage) (T, error) {
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		return t, err
	}

	return t, nil
}
