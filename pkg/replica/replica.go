package replica

import (
	"io"
	"log/slog"
	"sort"

	"github.com/tangledbytes/go-vsr/pkg/assert"
	"github.com/tangledbytes/go-vsr/pkg/events"
	"github.com/tangledbytes/go-vsr/pkg/ipv4port"
	"github.com/tangledbytes/go-vsr/pkg/log"
	"github.com/tangledbytes/go-vsr/pkg/network"
	"github.com/tangledbytes/go-vsr/pkg/queue"
	"github.com/tangledbytes/go-vsr/pkg/time"
)

type ReplicaStatus int

const (
	// ReplicaStatusNormal is the normal state of the replica.
	ReplicaStatusNormal ReplicaStatus = iota

	// ReplicaStatusViewChange is the state of the replica when it is
	// performing a view change.
	ReplicaStatusViewChange

	// ReplicaStatusRecovery is the state of the replica when it is
	// recovering from a failure.
	ReplicaStatusRecovery

	// ReplicaStatusStateTransfer is a made up state which is not mentioned
	// in the original VSR paper. This state is used to indicate that the
	// replica is performing a state transfer.
	ReplicaStatusStateTransfer
)

type InternalEventType = events.EventType

const (
	ProposeViewChange InternalEventType = iota + 100
)

type ClientTableData struct {
	Result        string
	RequestNumber uint64
	Finished      bool
}

type PendingPrepareOK struct {
	ClientAddr ipv4port.IPv4Port
	Request    events.Request
	Responses  map[uint64]struct{}
}

type PendingStartViewChange struct {
	Responses map[uint64]struct{}
}

type PendingDoViewChange struct {
	Responses map[uint64]events.DoViewChange
}

// VSRState represents state of a single replica in the VSR cluster.
// This replica could be Primary or Backup.
type VSRState struct {
	// ID is nothing but the index of the replica in the ClusterMembers list.
	ID uint64

	// ViewNumber is the current view number of the replica.
	ViewNumber uint64

	// CommitNumber is the monotonically increasing commit number of the replica.
	CommitNumber uint64

	// OpNum is the monotonically increasing operation number of the replica.
	OpNum uint64

	// ClientTable is a map of client IDs to the last operation number
	// that the client has seen.
	ClientTable map[uint64]ClientTableData

	// ClusterMembers is a list of the cluster members IPv4:port addresses.
	// The smallest IP address is the initial primary.
	ClusterMembers []ipv4port.IPv4Port

	// Logs is a list of the operations that have been requested by the clients.
	Logs log.Logs

	// Status is the current status of the replica.
	Status ReplicaStatus
}

type Internal struct {
	// pendingPOKs keep track of all the pending prepare OKs for a particular
	// operation number.
	pendingPOKs map[uint64]PendingPrepareOK
	// pendingStartViewChange keeps track of all the pending start view change
	// messages for a particular view number.
	pendingStartViewChange map[uint64]PendingStartViewChange
	// pendingDoViewChange keeps track of all the pending do view change
	// messages for a particular view number.
	pendingDoViewChange map[uint64]PendingDoViewChange

	// sq is the submission queue for the events that need to be processed.
	// everything in the replica is driven by this queue.
	sq *queue.Queue[events.NetworkEvent]

	// lastStableViewNumber is the last view number when the replica was in
	// stable state.
	lastStableViewNumber uint64
	// lastExecutedOpNum is the last operation number that the replica has
	// executed on the state machine. This number is tracked separately despite
	// the presence of commit number in the state because the commit number
	// changes in the view change protocol to the max commmit number but that
	// doesn't mean that this replica has executed all the operations till that
	lastExecutedOpNum uint64

	// viewChangeTimer is used to detect if the replica needs to initiate a
	// view change or not.
	viewChangeTimer *time.Timer
	// getStateTimer keeps track of the time when the replica might needs
	// to send another getState request to another replica as it didn't
	// hear from the previous replica.
	getStateTimer *time.Timer
	// commitTimer keeps track of the time when the primary (current) sent
	// the last commit message to the backups. If the primary didn't send
	// any commit message for a certain amount of time, it will do again.
	commitTimer *time.Timer

	// running is a flag that indicates if the replica is running or not.
	running bool
}

type Replica struct {
	// state holds all the VSR related state of the replica.
	state      VSRState
	network    network.Network
	time       time.Time
	logger     *slog.Logger
	srvhandler func(msg string) string

	heartbeattimeout uint64

	internal Internal
}

type Config struct {
	HeartbeatTimeout uint64
	SrvHandler       func(msg string) string
	Network          network.Network
	Time             time.Time
	ID               uint64
	Members          []string
	Logger           *slog.Logger
}

func New(cfg Config) (*Replica, error) {
	clustermembers := make([]ipv4port.IPv4Port, len(cfg.Members))
	for i, v := range cfg.Members {
		if err := clustermembers[i].FromHostPort(v); err != nil {
			return nil, err
		}
	}

	sort.Slice(clustermembers, func(i, j int) bool {
		return clustermembers[i].Less(clustermembers[j])
	})

	return &Replica{
		state: VSRState{
			ViewNumber:     0,
			CommitNumber:   0,
			ClientTable:    make(map[uint64]ClientTableData),
			Logs:           log.NewLogs(),
			Status:         ReplicaStatusNormal,
			ClusterMembers: clustermembers,
			ID:             cfg.ID,
		},
		network:          cfg.Network,
		time:             cfg.Time,
		srvhandler:       cfg.SrvHandler,
		heartbeattimeout: cfg.HeartbeatTimeout,
		logger:           cfg.Logger,
		internal: Internal{
			pendingPOKs:            make(map[uint64]PendingPrepareOK),
			pendingStartViewChange: make(map[uint64]PendingStartViewChange),
			pendingDoViewChange:    make(map[uint64]PendingDoViewChange),
			sq:                     queue.New[events.NetworkEvent](),

			commitTimer:     time.NewTimer(cfg.Time, cfg.HeartbeatTimeout),
			viewChangeTimer: time.NewTimer(cfg.Time, cfg.HeartbeatTimeout*2),
		},
	}, nil
}

// Submit will submit an event to the replica. This event will be eventually
// processed by the replica (after call to Run).
//
// This function should NOT be called from multiple threads.
func (r *Replica) Submit(e events.NetworkEvent) {
	assert.Assert(!r.internal.running, "replica should not be running")
	r.internal.sq.Push(e)
}

// Run will run replica event handling ONCE. It will dequeu one event from the
// submission queue and after processing it will return.
//
// It needs to be run in a loop to keep processing the events.
//
// This function should NOT be called from multiple threads.
func (r *Replica) Run() {
	assert.Assert(!r.internal.running, "replica should not be already running")

	r.setIsRunning(true)
	defer r.setIsRunning(false)

	r.consumeSQ()
	r.checkTimers()
}

// VSRState returns a copy of the VSR state of the replica.
func (r *Replica) VSRState() VSRState {
	return r.state
}

// InternalState returns a copy of the internal state of the replica.
func (r *Replica) InternalState() Internal {
	return r.internal
}

func (r *Replica) consumeSQ() {
	e, ok := r.internal.sq.Pop()
	if ok {
		r.processEvent(e)

		if r.isEventFromPrimary(&e) {
			r.logger.Debug("got event from primary", "event", e.Event.Type)
			r.internal.viewChangeTimer.Reset()
		}
	}
}

func (r *Replica) processEvent(e events.NetworkEvent) {
	switch e.Event.Type {
	case events.EventRequest:
		r.onRequest(e.Src, e.Event.Data.(events.Request))
	case events.EventPrepare:
		r.onPrepare(e.Src, e.Event.Data.(events.Prepare))
	case events.EventPrepareOK:
		r.onPrepareOK(e.Src, e.Event.Data.(events.PrepareOK))
	case events.EventCommit:
		r.onCommit(e.Src, e.Event.Data.(events.Commit))
	case events.EventStartViewChange:
		r.onStartViewChange(e.Src, e.Event.Data.(events.StartViewChange))
	case ProposeViewChange:
		r.initiateViewChange(r.state.ViewNumber + 1)
	case events.EventDoViewChange:
		r.onDoViewChange(e.Src, e.Event.Data.(events.DoViewChange))
	case events.EventStartView:
		r.onStartView(e.Src, e.Event.Data.(events.StartView))
	case events.EventGetState:
		r.onGetState(e.Src, e.Event.Data.(events.GetState))
	case events.EventNewState:
		r.onNewState(e.Src, e.Event.Data.(events.NewState))
	default:
		r.logger.Error("replica received an invalid event", "event", e.Event)
	}
}

// onRequest handles a request from a client. This is equivalent to
// <Request op, c, s> in the VSR-revisited paper.
func (r *Replica) onRequest(from ipv4port.IPv4Port, req events.Request) {
	r.logger.Debug("received request", "from", from.String(), "replica id", r.state.ID)

	// Am I in the state to perform the request?
	if r.state.Status != ReplicaStatusNormal {
		r.logger.Debug("not in normal state - dropping request", "state", r.state.Status)
		// I am not in the state to perform the request, drop the request.
		return
	}

	// Do I think I am primary? The client can have stale data but does that
	// stale data matches with my stale data?
	//
	// If it does matches and I proceed with the request, I am anyway not going
	// to get enough prepare OKs and the client request will timeout.
	if r.state.ID != r.potentialPrimary() {
		r.logger.Debug("not primary - dropping request", "id", r.state.ID, "potential primary", r.potentialPrimary())
		// I am not primary, drop the request

		// TODO: Forward the request to the primary
		return
	}

	// Check if we have already seen this request.
	client, ok := r.state.ClientTable[req.ClientID]
	if ok {
		r.logger.Debug("request received from known client", "client id", req.ClientID)
		if client.RequestNumber == req.ID {
			// We have already seen this request, send
			// the saved result
			r.logger.Debug("request duplicate request", "client id", req.ClientID, "request id", req.ID)

			if client.Finished {
				r.logger.Debug("request duplicate request (finished)", "client id", req.ClientID, "request id", req.ID)

				if err := r.sendReplyToClient(from, req.ClientID, req.ID, client.Result); err != nil {
					r.logger.Error("failed to send reply to client", "err", err, "client addr", from.String())
				}
			}

			// We have not finished processing this request, drop it
			// once we will finish it, we will send the result.
			return
		}

		if client.RequestNumber > req.ID {
			// We have already seen this request, but the
			// client has sent a stale request. Drop the request.
			return
		}
	}

	// At this point we know that we have not seen this request before. Proceed
	// to process this request.

	// 1. Increment the operation number.
	r.state.OpNum++
	// 2. Append the operation to the log.
	r.state.Logs.Add(log.Log{
		Data:      req.Op,
		ClientID:  req.ClientID,
		RequestID: req.ID,
	})

	// 3. Update the client table.
	r.state.ClientTable[req.ClientID] = ClientTableData{
		RequestNumber: client.RequestNumber,
	}

	// 4. Send the Prepare message to all the backups.
	for i, member := range r.state.ClusterMembers {
		if i == int(r.state.ID) {
			continue
		}

		r.logger.Debug("sending prepare to backup", "backup addr", member.String())

		if err := r.sendPrepareToBackup(member, req); err != nil {
			r.logger.Error("failed to send prepare to backup", "err", err, "backup addr", member.String())
		}
	}

	r.logger.Debug("sent prepare to all backups, wait for prepareOKs", "opNum", r.state.OpNum)

	// 5. Wait for prepareOKs
	r.internal.pendingPOKs[r.state.OpNum] = PendingPrepareOK{
		Request: req,
		Responses: map[uint64]struct{}{
			r.state.ID: {},
		},
		ClientAddr: from,
	}
}

// onPrepare handles prepare messages sent by primary to the backups.
func (r *Replica) onPrepare(from ipv4port.IPv4Port, ev events.Prepare) {
	r.logger.Debug("received prepare", "from", from.String(), "replica id", r.state.ID)

	if r.state.Status != ReplicaStatusNormal {
		// I am not in the state to process the message, drop the message.
		r.logger.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	if r.state.ID == r.potentialPrimary() {
		// I am the primary, drop the message.
		r.logger.Debug("primary - dropping request", "id", r.state.ID, "view num", r.state.ViewNumber)
		return
	}

	if r.state.ViewNumber > ev.ViewNum {
		// Poor old primary is behind (or network is horrible), drop the message.
		r.logger.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	if r.state.ViewNumber < ev.ViewNum {
		r.logger.Debug("replica lagging", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)

		// I am behind, initiate a state transfer.
		r.initiateStateTransfer(ev.ViewNum, ev.CommitNum)
		r.logger.Debug("initiated state transfer, dropping request")
		return
	}

	if r.state.OpNum+1 != ev.OpNum {
		r.logger.Debug("replica lagging", "received op num", ev.OpNum, "current op num", r.state.OpNum)

		// I am behind, initiate a state transfer.
		r.initiateStateTransfer(ev.ViewNum, ev.OpNum)
		r.logger.Debug("initiated state transfer, dropping request")
		return
	}

	// 1. Increment the operation number.
	r.state.OpNum++
	// 2. Add the operation to the log.
	r.state.Logs.Add(log.Log{
		Data:      ev.Request.Op,
		ClientID:  ev.Request.ClientID,
		RequestID: ev.Request.ID,
	})

	// 3. Update the client table
	r.state.ClientTable[ev.Request.ClientID] = ClientTableData{
		RequestNumber: ev.Request.ClientID,
	}

	// 4. Generate a fake Commit message and enqueue it.
	// This will be processed immediately after the prepare message.
	r.internal.sq.Push(events.NetworkEvent{
		Src: from,
		Event: &events.Event{
			Type: events.EventCommit,
			Data: events.Commit{
				ViewNum:   ev.ViewNum,
				CommitNum: ev.CommitNum,
			},
		},
	})

	// 4. Send the PrepareOK message to the primary.
	if err := r.sendPrepareOKToPrimary(from, ev.OpNum); err != nil {
		r.logger.Error("failed to send prepareOK to primary", "err", err, "primary addr", from.String())
		return
	}

	r.logger.Debug("sent prepareOK to primary", "opNum", ev.OpNum)
}

// onPrepareOK handles prepare ok messages sent by the backups to the primary.
// It will use the pendingPrepareOKs map to keep track of the number of prepare
// ok messages received for a particular operation number.
//
// This function does not check if the replica is a primary or not and expects
// to be invoked iff the replica is a primary. This check needs to be done by
// the caller.
func (r *Replica) onPrepareOK(from ipv4port.IPv4Port, pok events.PrepareOK) {
	r.logger.Debug("received prepareOK", "from", from.String(), "replica id", r.state.ID)

	if r.state.Status != ReplicaStatusNormal {
		r.logger.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	if r.state.ID != r.potentialPrimary() {
		r.logger.Debug("not primary - dropping request", "id", r.state.ID, "potential primary", r.potentialPrimary())
		return
	}

	// Check if this prepare OK is even relevant by checking the commit number
	if pok.OpNum <= r.state.CommitNumber {
		r.logger.Debug("prepareOK not relevant", "opNum", pok.OpNum, "commitNum", r.state.CommitNumber)
		return
	}

	// Record the prepare OK
	r.internal.pendingPOKs[pok.OpNum].Responses[pok.ReplicaID] = struct{}{}

	// Check if we have received prepare OKs from a majority of the replicas
	if len(r.internal.pendingPOKs[pok.OpNum].Responses) > len(r.state.ClusterMembers)/2 {
		// Process all the logs from last commit number to the op number
		// of the prepare OK.
		r.logger.Info("received prepareOK from majority", "opNum", pok.OpNum)

		for i := r.internal.lastExecutedOpNum + 1; i <= pok.OpNum; i++ {
			r.logger.Debug("processing log", "opNum", i, "log", r.state.Logs.OpAt(int(i)))

			// 1. Apply the operation to the state machine
			res := r.srvhandler(r.state.Logs.OpAt(int(i)))

			// 2. Update the commit number
			r.state.CommitNumber = i
			r.internal.lastExecutedOpNum = i

			// 3. Send the commit message to all the replicas
			for mi, member := range r.state.ClusterMembers {
				if r.state.ID == uint64(mi) {
					continue
				}

				// Send commits - not mentioned in the paper
				if err := r.sendCommitToBackup(member, i); err != nil {
					r.logger.Error("failed to send commit to backup",
						"err", err,
						"backup addr", member.String(),
						"opNum", i,
					)
				}
			}

			// 4. Store the result in the client table
			clientID := r.state.Logs.ClientIDAt(int(i))
			reqID := r.state.Logs.RequestIDAt(int(i))
			r.state.ClientTable[clientID] = ClientTableData{
				Result:        res,
				RequestNumber: reqID,
				Finished:      true,
			}

			// 5. Send the reply to the client
			r.logger.Debug("sending reply to client", "client addr", r.internal.pendingPOKs[i].ClientAddr.String())
			if r.internal.pendingPOKs[i].ClientAddr.String() != "" {
				if err := r.sendReplyToClient(
					r.internal.pendingPOKs[i].ClientAddr,
					clientID,
					reqID,
					res,
				); err != nil {
					r.logger.Error(
						"failed to send reply to client",
						"err", err,
						"client addr", r.internal.pendingPOKs[i].ClientAddr.String(),
						"request id ", r.internal.pendingPOKs[i].Request.ID,
					)
				}
			}

			// 6. Delete the pendingPrepareOKs entry
			delete(r.internal.pendingPOKs, i)
		}
	}
}

// onCommit handles commit messages sent by the primary to the backups.
//
// This function does not check if the replica is a backup or not and expects
// to be invoked iff the replica is a backup. This check needs to be done by
// the caller.
func (r *Replica) onCommit(from ipv4port.IPv4Port, commit events.Commit) {
	r.logger.Debug("received commit", "from", from.String(), "replica id", r.state.ID)

	if r.state.Status != ReplicaStatusNormal {
		r.logger.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	if r.state.ID == r.potentialPrimary() {
		r.logger.Debug("primary - dropping request", "id", r.state.ID, "view num", r.state.ViewNumber)
		return
	}

	if r.state.ViewNumber > commit.ViewNum {
		r.logger.Debug("old view number", "received view num", commit.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	if r.state.ViewNumber < commit.ViewNum {
		r.logger.Debug("replica lagging", "received view num", commit.ViewNum, "current view num", r.state.ViewNumber)
		r.initiateStateTransfer(commit.ViewNum, commit.CommitNum)
		r.logger.Debug("initiated state transfer, dropping request")
		return
	}

	if r.state.OpNum < commit.CommitNum {
		r.logger.Debug("replica lagging", "received commit num", commit.CommitNum, "current op num", r.state.OpNum)
		r.initiateStateTransfer(commit.ViewNum, commit.CommitNum)
		r.logger.Debug("initiated state transfer, dropping request")
		return
	}

	// Process all the logs from last commit number to the commit number
	for i := r.internal.lastExecutedOpNum + 1; i <= commit.CommitNum; i++ {
		r.logger.Debug("processing log", "opNum", i, "log", r.state.Logs.OpAt(int(i)))

		// 1. Apply the operation to the state machine
		res := r.srvhandler(r.state.Logs.OpAt(int(i)))

		// 2. Update the commit number
		r.state.CommitNumber = i
		r.internal.lastExecutedOpNum = i

		// 3. Store the result in the client table
		clientID := r.state.Logs.ClientIDAt(int(i))
		reqID := r.state.Logs.RequestIDAt(int(i))
		r.state.ClientTable[clientID] = ClientTableData{
			Result:        res,
			RequestNumber: reqID,
			Finished:      true,
		}
	}
}

func (r *Replica) onStartViewChange(src ipv4port.IPv4Port, ev events.StartViewChange) {
	r.logger.Debug("received startViewChange", "from", src.String(), "replica id", r.state.ID)
	r.internal.viewChangeTimer.Reset()
	// Total 6 states are possible (2 replica states) and (3 view number relations)

	// Cover 2 states, normal and view change when the view number is lesser
	// then ours
	// TOTAL: 2
	if ev.ViewNum < r.state.ViewNumber {
		r.logger.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	// Cover 1 state
	// TOTAL: 3
	if r.state.Status == ReplicaStatusNormal && ev.ViewNum == r.state.ViewNumber {
		r.logger.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	// If I just received a initiation for a view change (with higher number) and I am not in the
	// view change state then I need to initiate a view change.
	//
	// Cover 2 state
	// TOTAL: 5
	if ev.ViewNum > r.state.ViewNumber {
		r.logger.Info("initiating view change", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)

		r.initiateViewChange(ev.ViewNum)
		// Don't return here as we need to process the start view change message
	}

	// Already in view change status and the view number matches
	//
	// Cover 1 state
	// TOTAL: 6
	if r.state.Status == ReplicaStatusViewChange && ev.ViewNum == r.state.ViewNumber {
		_, ok := r.internal.pendingStartViewChange[ev.ViewNum]
		if !ok {
			r.internal.pendingStartViewChange[ev.ViewNum] = PendingStartViewChange{
				Responses: map[uint64]struct{}{},
			}
		}

		r.internal.pendingStartViewChange[ev.ViewNum].Responses[ev.ReplicaID] = struct{}{}

		// Check if we have received view change messages from a majority of the replicas
		if len(r.internal.pendingStartViewChange[ev.ViewNum].Responses) > len(r.state.ClusterMembers)/2 {
			r.logger.Info("received view change from majority", "view num", ev.ViewNum)

			if err := r.sendDoViewChangeToPrimary(r.state.ClusterMembers[r.potentialPrimary()]); err != nil {
				r.logger.Error("failed to send doViewChange to primary", "err", err)
			}

			r.logger.Debug("sent doViewChange to primary", "view num", ev.ViewNum)
		}

		return
	}

	assert.Assert(false, "should be unreachable state")
}

func (r *Replica) onDoViewChange(src ipv4port.IPv4Port, ev events.DoViewChange) {
	r.logger.Debug("received doViewChange", "from", src.String(), "replica id", r.state.ID)
	r.internal.viewChangeTimer.Reset()

	if r.state.Status != ReplicaStatusViewChange {
		r.logger.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	// Don't do primary test here as it could be that we missed the start view change
	// and just getting to know about it now but for sanity ensure that the
	// view number is higher than the current view number.
	if ev.ViewNum < r.state.ViewNumber {
		r.logger.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	_, ok := r.internal.pendingDoViewChange[ev.ViewNum]
	if !ok {
		r.internal.pendingDoViewChange[ev.ViewNum] = PendingDoViewChange{
			Responses: map[uint64]events.DoViewChange{},
		}
	}

	r.internal.pendingDoViewChange[ev.ViewNum].Responses[ev.ReplicaID] = ev

	// Check if we have received do view change messages from a majority of the replicas
	if len(r.internal.pendingDoViewChange[ev.ViewNum].Responses) > len(r.state.ClusterMembers)/2 {
		r.logger.Info("received doViewChange from majority", "view num", ev.ViewNum)

		r.finalizeViewChange()

		for mi, member := range r.state.ClusterMembers {
			if r.state.ID == uint64(mi) {
				continue
			}

			r.logger.Debug("sending startView to backup", "backup addr", member.String())
			if err := r.sendStartViewToBackup(member); err != nil {
				r.logger.Error("failed to send startView to backup", "err", err, "backup addr", member.String())
			}
		}

		r.logger.Info("view change complete", "view num", ev.ViewNum)
	}
}

func (r *Replica) onStartView(src ipv4port.IPv4Port, ev events.StartView) {
	r.logger.Debug("received startView", "from", src.String(), "replica id", r.state.ID)

	if ev.ViewNum < r.state.ViewNumber {
		r.logger.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	r.state.OpNum = ev.OpNum
	r.state.Logs = ev.Logs
	r.state.CommitNumber = ev.CommitNum
	r.state.ViewNumber = ev.ViewNum
	r.state.Status = ReplicaStatusNormal

	// Send PrepareOK for all the non committed logs
	for i := ev.CommitNum + 1; i <= ev.OpNum; i++ {
		r.state.ClientTable[ev.Logs.ClientIDAt(int(i))] = ClientTableData{
			RequestNumber: ev.Logs.RequestIDAt(int(i)),
		}

		r.logger.Debug("sending prepareOK to primary", "opNum", i)

		if err := r.sendPrepareOKToPrimary(r.state.ClusterMembers[r.potentialPrimary()], i); err != nil {
			r.logger.Error("failed to send prepareOK to primary", "err", err, "primary addr", r.state.ClusterMembers[r.potentialPrimary()].String())
		}
	}

	delete(r.internal.pendingStartViewChange, ev.ViewNum)
	delete(r.internal.pendingDoViewChange, ev.ViewNum)

	// The primary will eventually send either Commit message or Prepare which
	// will allow the backup to execute the operations against the state machine
}

func (r *Replica) onGetState(src ipv4port.IPv4Port, ev events.GetState) {
	r.logger.Warn("received getState", "from", src.String(), "replica id", r.state.ID)
	if r.state.Status != ReplicaStatusNormal {
		r.logger.Warn("not in state - dropping request", "state", r.state.Status)
		return
	}

	if r.state.ViewNumber != ev.ViewNum {
		r.logger.Warn("not current view", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	// Not mentioned in the paper but we need to check if the requested op number is
	// lesser than the current op number only then we will respond with the state.
	if r.state.OpNum < ev.OpNum {
		r.logger.Warn("not current op num", "received op num", ev.OpNum, "current op num", r.state.OpNum, "id", r.state.ID)
		return
	}

	if err := r.sendNewState(src, int(ev.OpNum)); err != nil {
		r.logger.Error("failed to send newState to replica", "err", err, "replica addr", src.String())
	}
}

func (r *Replica) onNewState(src ipv4port.IPv4Port, ev events.NewState) {
	r.logger.Warn("received newState", "from", src.String(), "replica id", r.state.ID)

	if r.state.Status != ReplicaStatusStateTransfer {
		r.logger.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	r.state.ViewNumber = ev.ViewNum
	r.state.OpNum = ev.OpNum
	r.state.Logs.Merge(ev.Logs)
	r.state.Status = ReplicaStatusNormal

	r.internal.getStateTimer = nil
}

func (r *Replica) initiateViewChange(viewnum uint64) {
	r.logger.Debug("initiating view change", "view num", viewnum)

	r.state.Status = ReplicaStatusViewChange
	r.internal.lastStableViewNumber = r.state.ViewNumber
	r.state.ViewNumber = viewnum

	// Remove relevant timers
	r.internal.getStateTimer = nil

	for _, member := range r.state.ClusterMembers {
		r.logger.Debug("sending startViewChange to replica", "replica addr", member.String(), "view num", viewnum)
		if err := r.sendStartViewChange(member, viewnum); err != nil {
			r.logger.Error("failed to send startViewChange to replica", "err", err, "replica addr", member.String())
		}
	}
}

func (r *Replica) initiateStateTransfer(newViewNum, newOpNum uint64) {
	r.logger.Debug(
		"initiate state transfer",
		"view num", newViewNum,
		"current view num", r.state.ViewNumber,
		"op num", newOpNum,
		"current op num", r.state.OpNum,
	)

	if r.state.Status != ReplicaStatusNormal {
		// Remove the timer, as we might have entered a view change state or
		// recovery state. If a state transfer would still be needed, next calls
		// to prepare or commit will trigger it.
		r.internal.getStateTimer = nil
		return
	}

	// Now that we know that we are in desired state, lets change the status of the replica
	// to ensure that we do not process any Prepare or Commit messages while we are trying
	// to perform the state transfer.
	// ViewChange protocol will still be able to proceed as `onStartViewChange` does not
	// care about any status. This is desire as we don't want to race against the state
	// updates from view change.
	r.state.Status = ReplicaStatusStateTransfer

	if r.state.ViewNumber < newViewNum {
		// Hard luck, gotta trucate
		// 1. Set op number to the commit number
		r.state.OpNum = r.state.CommitNumber
		// 2. Truncate the logs
		r.state.Logs.TruncateAfter(int(r.state.CommitNumber))
	}

	targetReplica := r.primaryForView(newViewNum)
	// If the timer is set that means we have been here and probably attempted
	// to perform the state transfer but failed. In that case we need to try
	// the next replica in the cluster members list.
	//
	// NOTE: if view change protocol kicks then this timer should be removed!
	if r.internal.getStateTimer != nil {
		state, ok := r.internal.getStateTimer.State()
		assert.Assert(ok, "getStateTime state should be set")

		castedState := state.([3]uint64)
		targetReplica = (castedState[2] + 1) % uint64(len(r.state.ClusterMembers))
	}

	// 3. Set the timer to ensure that we get the state within the timeout
	r.internal.getStateTimer = time.NewTimer(r.time, 15*time.SECOND)
	r.internal.getStateTimer.SetState([3]uint64{newViewNum, newOpNum, targetReplica})

	// 4. Send GetState to the replica from which we received the request
	//
	// NOTE: it might be better to send the requests to every cluster member and then
	// wait for the first response. This will ensure that we get the state from the
	// fastest replica.
	//
	// This isn't done here because the logs are in memory and the simulator simply
	// cannot manage this much memory being moved around :(
	if err := r.sendGetStateToReplica(r.state.ClusterMembers[targetReplica]); err != nil {
		r.logger.Error("failed to send getState to replica", "err", err, "replica addr", r.state.ClusterMembers[targetReplica].String())
	}
}

func (r *Replica) sendPrepareToBackup(backup ipv4port.IPv4Port, req events.Request) error {
	return r.network.Send(backup, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventPrepare,
			Data: events.Prepare{
				ViewNum:   r.state.ViewNumber,
				OpNum:     r.state.OpNum,
				CommitNum: r.state.CommitNumber,
				Request:   req,
				ReplicaID: r.state.ID,
			},
		}

		return ev.ToWriter(w)
	})
}

func (r *Replica) sendReplyToClient(client ipv4port.IPv4Port, clientID, reqID uint64, res string) error {
	return r.network.Send(client, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventReply,
			Data: events.Reply{
				ViewNum:  r.state.ViewNumber,
				ID:       reqID,
				ClientID: clientID,
				Result:   res,
			},
		}

		return ev.ToWriter(w)
	})
}

func (r *Replica) sendPrepareOKToPrimary(primary ipv4port.IPv4Port, opnum uint64) error {
	return r.network.Send(primary, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventPrepareOK,
			Data: events.PrepareOK{
				ViewNum:   r.state.ViewNumber,
				OpNum:     opnum,
				ReplicaID: r.state.ID,
			},
		}

		return ev.ToWriter(w)
	})
}

func (r *Replica) sendCommitToBackup(backup ipv4port.IPv4Port, commitnum uint64) error {
	return r.network.Send(backup, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventCommit,
			Data: events.Commit{
				ViewNum:   r.state.ViewNumber,
				CommitNum: commitnum,
				ReplicaID: r.state.ID,
			},
		}

		return ev.ToWriter(w)
	})
}

func (r *Replica) sendStartViewChange(backup ipv4port.IPv4Port, viewNum uint64) error {
	return r.network.Send(backup, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventStartViewChange,
			Data: events.StartViewChange{
				ViewNum:   viewNum,
				ReplicaID: r.state.ID,
			},
		}

		return ev.ToWriter(w)
	})
}

func (r *Replica) sendDoViewChangeToPrimary(primary ipv4port.IPv4Port) error {
	return r.network.Send(primary, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventDoViewChange,
			Data: events.DoViewChange{
				ViewNum:           r.state.ViewNumber,
				Logs:              r.state.Logs,
				LastStableViewNum: r.internal.lastStableViewNumber,
				OpNum:             r.state.OpNum,
				CommitNum:         r.state.CommitNumber,
				ReplicaID:         r.state.ID,
			},
		}

		return ev.ToWriter(w)
	})
}

func (r *Replica) sendStartViewToBackup(backup ipv4port.IPv4Port) error {
	return r.network.Send(backup, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventStartView,
			Data: events.StartView{
				ViewNum:   r.state.ViewNumber,
				OpNum:     r.state.OpNum,
				CommitNum: r.state.CommitNumber,
				Logs:      r.state.Logs,
				ReplicaID: r.state.ID,
			},
		}

		return ev.ToWriter(w)
	})
}

func (r *Replica) sendGetStateToReplica(replica ipv4port.IPv4Port) error {
	return r.network.Send(replica, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventGetState,
			Data: events.GetState{
				ViewNum:   r.state.ViewNumber,
				OpNum:     r.state.OpNum,
				ReplicaID: r.state.ID,
			},
		}

		return ev.ToWriter(w)
	})
}

func (r *Replica) sendNewState(replica ipv4port.IPv4Port, logsFrom int) error {
	return r.network.Send(replica, func(w io.Writer) error {
		ev := events.Event{
			Type: events.EventNewState,
			Data: events.NewState{
				ViewNum:   r.state.ViewNumber,
				OpNum:     r.state.OpNum,
				CommitNum: r.state.CommitNumber,
				Logs:      r.state.Logs.After(logsFrom),
				ReplicaID: r.state.ID,
			},
		}

		return ev.ToWriter(w)
	})
}

// finalizeViewChange finalizes the view change for this node which
// will make this node the new primary
//
// This should NOT be called until the node has received at least `f`
// DoViewChange messages from the replicas AND one from itself (important
// for correctness IMO).
func (r *Replica) finalizeViewChange() {
	r.state.ViewNumber = r.largestViewNumInPendingViewChange()

	bestLastStableView := uint64(0)
	bestLogsCandidates := []events.DoViewChange{}
	for _, v := range r.internal.pendingDoViewChange[r.state.ViewNumber].Responses {
		if v.LastStableViewNum >= bestLastStableView {
			bestLastStableView = v.LastStableViewNum
			bestLogsCandidates = append(bestLogsCandidates, v)
		}
	}

	bestOpNum := uint64(0)
	bestLogs := []log.Log{}
	bestCommit := uint64(0)
	for _, v := range bestLogsCandidates {
		if v.OpNum >= bestOpNum {
			bestOpNum = v.OpNum
			bestLogs = v.Logs
		}
		if v.CommitNum > bestCommit {
			bestCommit = v.CommitNum
		}
	}

	r.state.OpNum = bestOpNum
	r.state.Logs = bestLogs
	r.state.CommitNumber = bestCommit
	r.state.Status = ReplicaStatusNormal

	// Need to seed tables
	for i := r.state.CommitNumber + 1; i <= r.state.OpNum; i++ {
		r.state.ClientTable[r.state.Logs.ClientIDAt(int(i))] = ClientTableData{
			RequestNumber: r.state.Logs.RequestIDAt(int(i)),
		}

		r.internal.pendingPOKs[i] = PendingPrepareOK{
			Responses: map[uint64]struct{}{
				r.state.ID: {},
			},
		}
	}
}

func (r *Replica) checkTimers() {
	r.checkGetStateTimeout()
	r.checkCommitTimeout()
	r.checkViewChangeTimer()
}

func (r *Replica) checkViewChangeTimer() {
	if r.potentialPrimary() != r.state.ID {
		return
	}

	if !r.internal.viewChangeTimer.Done() {
		return
	}

	r.logger.Debug("view change timer timed out", "view num", r.state.ViewNumber)
	r.internal.sq.Push(events.NetworkEvent{
		Event: &events.Event{
			Type: InternalEventType(ProposeViewChange),
		},
	})

	r.internal.viewChangeTimer.Reset()
}

func (r *Replica) checkCommitTimeout() {
	if !r.internal.commitTimer.Done() {
		return
	}

	if r.state.ID != r.potentialPrimary() {
		return
	}

	r.logger.Debug(
		"broadcasting commit message",
		"commit num", r.state.CommitNumber,
		"view num", r.state.ViewNumber,
		"replica id", r.state.ID,
	)

	for mi, member := range r.state.ClusterMembers {
		if r.state.ID == uint64(mi) {
			continue
		}

		// It's OK if the broadcast fails
		r.sendCommitToBackup(member, r.state.CommitNumber)
	}

	r.internal.commitTimer.Reset()
}

func (r *Replica) checkGetStateTimeout() {
	if r.internal.getStateTimer == nil {
		return
	}

	if !r.internal.getStateTimer.Done() {
		return
	}

	state, ok := r.internal.getStateTimer.State()
	assert.Assert(ok, "state should not be nil for getStateTimer")

	// Yes, I want this to panic if the state is not of the type I expect
	// as this is a bug in the code.
	// The expected state is [3]uint64 - [viewNum, opNum, lastReplicaIDTried]
	castedState := state.([3]uint64)

	r.logger.Warn("getState timed out", "state", state)
	r.initiateStateTransfer(castedState[0], castedState[1])
}

func (r *Replica) potentialPrimary() uint64 {
	return r.primaryForView(r.state.ViewNumber)
}

func (r *Replica) primaryForView(num uint64) uint64 {
	return uint64(int(num) % len(r.state.ClusterMembers))
}

func (r *Replica) isEventFromPrimary(e *events.NetworkEvent) bool {
	if e.Event.Type == events.EventPrepare {
		ev := e.Event.Data.(events.Prepare)
		return ev.ReplicaID == r.potentialPrimary()
	}

	if e.Event.Type == events.EventCommit {
		ev := e.Event.Data.(events.Commit)
		return ev.ReplicaID == r.potentialPrimary()
	}

	if e.Event.Type == events.EventStartView {
		ev := e.Event.Data.(events.StartView)
		return ev.ReplicaID == r.potentialPrimary()
	}

	return false
}

func (r *Replica) largestViewNumInPendingViewChange() uint64 {
	largest := r.state.ViewNumber
	for v := range r.internal.pendingDoViewChange {
		if v > largest {
			largest = v
		}
	}

	return largest
}

func (r *Replica) setIsRunning(isRunning bool) {
	r.internal.running = isRunning
}
