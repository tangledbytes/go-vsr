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
	EventGetState
	EventNewState
	EventHeartbeat
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

type GetState struct {
	ViewNum   uint64 `json:"view_num,string"`
	OpNum     uint64 `json:"op_num,string"`
	ReplicaID uint64 `json:"replica_id,string"`
}

type NewState struct {
	ViewNum   uint64   `json:"view_num,string"`
	Logs      log.Logs `json:"logs"`
	CommitNum uint64   `json:"commit_num,string"`
	OpNum     uint64   `json:"op_num,string"`
	ReplicaID uint64   `json:"replica_id,string"`
}

type Heartbeat struct{}

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
		var v Request
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventPrepare:
		var v Prepare
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventPrepareOK:
		var v PrepareOK
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventCommit:
		var v Commit
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventStartViewChange:
		var v StartViewChange
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventDoViewChange:
		var v DoViewChange
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventStartView:
		var v StartView
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventGetState:
		var v GetState
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventNewState:
		var v NewState
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventHeartbeat:
		var v Heartbeat
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
	case EventReply:
		var v Reply
		err = json.Unmarshal(temp.Data, &v)
		ev.Data = v
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
