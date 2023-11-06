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
)

type internalEventType = events.EventType

const (
	proposeViewChange = iota + 100
)

type clientTableData struct {
	Result        string
	RequestNumber uint64
	Finished      bool
}

type pendingPrepareOK struct {
	ClientAddr ipv4port.IPv4Port
	Request    events.Request
	Responses  map[uint64]struct{}
}

type pendingStartViewChange struct {
	Responses map[uint64]struct{}
}

type pendingDoViewChange struct {
	Responses map[uint64]events.DoViewChange
}

// vsrState represents state of a single replica in the VSR cluster.
// This replica could be Primary or Backup.
type vsrState struct {
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
	ClientTable map[uint64]clientTableData

	// ClusterMembers is a list of the cluster members IPv4:port addresses.
	// The smallest IP address is the initial primary.
	ClusterMembers []ipv4port.IPv4Port

	// Logs is a list of the operations that have been requested by the clients.
	//
	// VSR has the component called opnumber, instead of opnumber we use
	// the index of the log entry in the Logs list as the opnumber.
	Logs log.Logs

	// Status is the current status of the replica.
	Status ReplicaStatus
}

type internal struct {
	// pendingPOKs keep track of all the pending prepare OKs for a particular
	// operation number.
	pendingPOKs map[uint64]pendingPrepareOK
	// pendingStartViewChange keeps track of all the pending start view change
	// messages for a particular view number.
	pendingStartViewChange map[uint64]pendingStartViewChange
	pendingDoViewChange    map[uint64]pendingDoViewChange

	// sq is the submission queue for the events that need to be processed.
	// everything in the replica is driven by this queue.
	sq *queue.Queue[events.NetworkEvent]

	// viewChangeTimeout is the last time when the replica heard from the
	// primary. This is only relevant for the backups.
	viewChangeTimeout uint64
	// lastPingedBackup is the last time when the replica pinged the backup.
	// This is only relevant for the primary.
	lastPingedBackup []uint64
	// lastStableViewNumber is the last view number when the replica was in
	// stable state.
	lastStableViewNumber uint64
	// lastExecutedOpNum is the last operation number that the replica has
	// executed on the state machine. This number is tracked separately despite
	// the presence of commit number in the state because the commit number
	// changes in the view change protocol to the max commmit number but that
	// doesn't mean that this replica has executed all the operations till that
	lastExecutedOpNum uint64
}

type Replica struct {
	// state holds all the VSR related state of the replica.
	state      vsrState
	network    network.Network
	time       time.Time
	srvhandler func(msg string) string

	heartbeattimeout uint64

	internal internal
}

type Config struct {
	HeartbeatTimeout uint64
	SrvHandler       func(msg string) string
	Network          network.Network
	Time             time.Time
	ID               int
	Members          []string
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
		state: vsrState{
			ViewNumber:     0,
			CommitNumber:   0,
			ClientTable:    make(map[uint64]clientTableData),
			Logs:           log.NewLogs(),
			Status:         ReplicaStatusNormal,
			ClusterMembers: clustermembers,
			ID:             uint64(cfg.ID),
		},
		network:          cfg.Network,
		time:             cfg.Time,
		srvhandler:       cfg.SrvHandler,
		heartbeattimeout: cfg.HeartbeatTimeout,
		internal: internal{
			pendingPOKs:            make(map[uint64]pendingPrepareOK),
			pendingStartViewChange: make(map[uint64]pendingStartViewChange),
			pendingDoViewChange:    make(map[uint64]pendingDoViewChange),
			sq:                     queue.New[events.NetworkEvent](),
			viewChangeTimeout:      cfg.Time.Now(),
			lastPingedBackup:       make([]uint64, len(clustermembers)),
		},
	}, nil
}

// Submit will submit an event to the replica. This event will be eventually
// processed by the replica (after call to Run).
//
// This function is NOT thread safe.
func (r *Replica) Submit(e events.NetworkEvent) {
	r.internal.sq.Push(e)
}

// Run will run replica event handling ONCE. It will dequeu one event from the
// submission queue and after processing it will return.
//
// It needs to be run in a loop to keep processing the events.
//
// This function is NOT thread safe.
func (r *Replica) Run() {
	e, ok := r.internal.sq.Pop()
	if ok {
		r.processEvent(e)

		if r.isEventFromPrimary(&e) {
			slog.Debug("got event from primary", "event", e.Event.Type)
			r.internal.viewChangeTimeout = r.time.Now()
		}
	}

	r.detectViewChangeNeed()
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
	case proposeViewChange:
		r.initiateViewChange(r.state.ViewNumber + 1)
	case events.EventDoViewChange:
		r.onDoViewChange(e.Src, e.Event.Data.(events.DoViewChange))
	case events.EventStartView:
		r.onStartView(e.Src, e.Event.Data.(events.StartView))
	default:
		slog.Error("replica received an invalid event", "event", e.Event)
	}
}

// onRequest handles a request from a client. This is equivalent to
// <Request op, c, s> in the VSR-revisited paper.
func (r *Replica) onRequest(from ipv4port.IPv4Port, req events.Request) {
	slog.Debug("received request", "from", from.String())

	// Am I in the state to perform the request?
	if r.state.Status != ReplicaStatusNormal {
		slog.Debug("not in normal state - dropping request", "state", r.state.Status)
		// I am not in the state to perform the request, drop the request.
		return
	}

	// Do I think I am primary? The client can have stale data but does that
	// stale data matches with my stale data?
	//
	// If it does matches and I proceed with the request, I am anyway not going
	// to get enough prepare OKs and the client request will timeout.
	if r.state.ID != r.potentialPrimary() {
		slog.Debug("not primary - dropping request", "id", r.state.ID, "potential primary", r.potentialPrimary())
		// I am not primary, drop the request

		// TODO: Forward the request to the primary
		return
	}

	// Check if we have already seen this request.
	client, ok := r.state.ClientTable[req.ClientID]
	if ok {
		slog.Debug("request received from known client", "client id", req.ClientID)
		if client.RequestNumber == req.ID {
			// We have already seen this request, send
			// the saved result
			slog.Debug("request duplicate request", "client id", req.ClientID, "request id", req.ID)

			if client.Finished {
				slog.Debug("request duplicate request (finished)", "client id", req.ClientID, "request id", req.ID)

				if err := r.sendReplyToClient(from, req.ClientID, req.ID, client.Result); err != nil {
					slog.Error("failed to send reply to client", "err", err, "client addr", from.String())
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
	r.state.ClientTable[req.ClientID] = clientTableData{
		RequestNumber: client.RequestNumber,
	}

	// 4. Send the Prepare message to all the backups.
	for i, member := range r.state.ClusterMembers {
		if i == int(r.state.ID) {
			continue
		}

		slog.Debug("sending prepare to backup", "backup addr", member.String())

		if err := r.sendPrepareToBackup(member, req); err != nil {
			slog.Error("failed to send prepare to backup", "err", err, "backup addr", member.String())
		}
	}

	slog.Debug("sent prepare to all backups, wait for prepareOKs", "opNum", r.state.OpNum)

	// 5. Wait for prepareOKs
	r.internal.pendingPOKs[r.state.OpNum] = pendingPrepareOK{
		Request: req,
		Responses: map[uint64]struct{}{
			r.state.ID: {},
		},
		ClientAddr: from,
	}
}

// onPrepare handles prepare messages sent by primary to the backups.
func (r *Replica) onPrepare(from ipv4port.IPv4Port, ev events.Prepare) {
	slog.Debug("received prepare", "from", from.String())

	if r.state.Status != ReplicaStatusNormal {
		// I am not in the state to process the message, drop the message.
		slog.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	if r.state.ID == r.potentialPrimary() {
		// I am the primary, drop the message.
		slog.Debug("primary - dropping request", "id", r.state.ID, "view num", r.state.ViewNumber)
		return
	}

	if r.state.ViewNumber > ev.ViewNum {
		// Poor old primary is behind (or network is horrible), drop the message.
		slog.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	if r.state.ViewNumber < ev.ViewNum {
		slog.Debug("replica lagging", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)

		// I am behind, initiate a state transfer.
		if err := r.performStateTransfer(); err != nil {
			// If we can't perform the state transfer, drop the message.
			return
		}

		slog.Debug("state transfer complete", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
	}

	if r.state.OpNum+1 != ev.OpNum {
		slog.Debug("replica lagging", "received op num", ev.OpNum, "current op num", r.state.OpNum)

		// I am behind, initiate a state transfer.
		if err := r.performStateTransfer(); err != nil {
			// If we can't perform the state transfer, drop the message.
			return
		}

		slog.Debug("state transfer complete", "received op num", ev.OpNum, "current op num", r.state.OpNum)
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
	r.state.ClientTable[ev.Request.ClientID] = clientTableData{
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
		slog.Error("failed to send prepareOK to primary", "err", err, "primary addr", from.String())
		return
	}

	slog.Debug("sent prepareOK to primary", "opNum", ev.OpNum)
}

// onPrepareOK handles prepare ok messages sent by the backups to the primary.
// It will use the pendingPrepareOKs map to keep track of the number of prepare
// ok messages received for a particular operation number.
//
// This function does not check if the replica is a primary or not and expects
// to be invoked iff the replica is a primary. This check needs to be done by
// the caller.
func (r *Replica) onPrepareOK(from ipv4port.IPv4Port, pok events.PrepareOK) {
	slog.Debug("received prepareOK", "from", from.String())

	if r.state.Status != ReplicaStatusNormal {
		slog.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	if r.state.ID != r.potentialPrimary() {
		slog.Debug("not primary - dropping request", "id", r.state.ID, "potential primary", r.potentialPrimary())
		return
	}

	// Check if this prepare OK is even relevant by checking the commit number
	if pok.OpNum <= r.state.CommitNumber {
		slog.Debug("prepareOK not relevant", "opNum", pok.OpNum, "commitNum", r.state.CommitNumber)
		return
	}

	// Record the prepare OK
	r.internal.pendingPOKs[pok.OpNum].Responses[pok.ReplicaID] = struct{}{}

	// Check if we have received prepare OKs from a majority of the replicas
	if len(r.internal.pendingPOKs[pok.OpNum].Responses) > len(r.state.ClusterMembers)/2 {
		// Process all the logs from last commit number to the op number
		// of the prepare OK.
		slog.Info("received prepareOK from majority", "opNum", pok.OpNum)

		for i := r.internal.lastExecutedOpNum + 1; i <= pok.OpNum; i++ {
			slog.Debug("processing log", "opNum", i, "log", r.state.Logs.OpAt(int(i)))

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

				if r.time.Now()-r.internal.lastPingedBackup[mi] < r.heartbeattimeout {
					continue
				}

				slog.Debug(
					"sending commit to backup",
					"backup addr", member.String(),
					"last heard", r.internal.lastPingedBackup[mi],
					"opNum", i,
				)

				if err := r.sendCommitToBackup(member, i); err != nil {
					slog.Error("failed to send commit to backup",
						"err", err,
						"backup addr", member.String(),
						"opNum", i,
					)
				}
			}

			// 4. Store the result in the client table
			clientID := r.state.Logs.ClientIDAt(int(i))
			reqID := r.state.Logs.RequestIDAt(int(i))
			r.state.ClientTable[clientID] = clientTableData{
				Result:        res,
				RequestNumber: reqID,
				Finished:      true,
			}

			// 5. Send the reply to the client
			slog.Debug("sending reply to client", "client addr", r.internal.pendingPOKs[i].ClientAddr.String())
			if r.internal.pendingPOKs[i].ClientAddr.String() != "" {
				if err := r.sendReplyToClient(
					r.internal.pendingPOKs[i].ClientAddr,
					clientID,
					reqID,
					res,
				); err != nil {
					slog.Error(
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
	slog.Debug("received commit", "from", from.String())

	if r.state.Status != ReplicaStatusNormal {
		slog.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	if r.state.ID == r.potentialPrimary() {
		slog.Debug("primary - dropping request", "id", r.state.ID, "view num", r.state.ViewNumber)
		return
	}

	if r.state.ViewNumber > commit.ViewNum {
		slog.Debug("old view number", "received view num", commit.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	if r.state.ViewNumber < commit.ViewNum {
		slog.Debug("replica lagging", "received view num", commit.ViewNum, "current view num", r.state.ViewNumber)
		if err := r.performStateTransfer(); err != nil {
			slog.Error("failed to perform state transfer", "err", err)
			return
		}

		slog.Debug("state transfer complete", "received view num", commit.ViewNum, "current view num", r.state.ViewNumber)
	}

	if r.state.OpNum < commit.CommitNum {
		slog.Debug("replica lagging", "received commit num", commit.CommitNum, "current op num", r.state.OpNum)
		if err := r.performStateTransfer(); err != nil {
			slog.Error("failed to perform state transfer", "err", err)
			return
		}

		slog.Debug("state transfer complete", "received commit num", commit.CommitNum, "current op num", r.state.OpNum)
	}

	// Process all the logs from last commit number to the commit number
	for i := r.internal.lastExecutedOpNum + 1; i <= commit.CommitNum; i++ {
		slog.Debug("processing log", "opNum", i, "log", r.state.Logs.OpAt(int(i)))

		// 1. Apply the operation to the state machine
		res := r.srvhandler(r.state.Logs.OpAt(int(i)))

		// 2. Update the commit number
		r.state.CommitNumber = i
		r.internal.lastExecutedOpNum = i

		// 3. Store the result in the client table
		clientID := r.state.Logs.ClientIDAt(int(i))
		reqID := r.state.Logs.RequestIDAt(int(i))
		r.state.ClientTable[clientID] = clientTableData{
			Result:        res,
			RequestNumber: reqID,
			Finished:      true,
		}
	}
}

func (r *Replica) onStartViewChange(src ipv4port.IPv4Port, ev events.StartViewChange) {
	slog.Debug("received startViewChange", "from", src.String())
	r.internal.viewChangeTimeout = r.time.Now()
	// Total 6 states are possible (2 replica states) and (3 view number relations)

	// Cover 2 states, normal and view change when the view number is lesser
	// then ours
	// TOTAL: 2
	if ev.ViewNum < r.state.ViewNumber {
		slog.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	// Cover 1 state
	// TOTAL: 3
	if r.state.Status == ReplicaStatusNormal && ev.ViewNum == r.state.ViewNumber {
		slog.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	// If I just received a initiation for a view change (with higher number) and I am not in the
	// view change state then I need to initiate a view change.
	//
	// Cover 2 state
	// TOTAL: 4
	if ev.ViewNum > r.state.ViewNumber {
		slog.Info("initiating view change", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)

		r.initiateViewChange(ev.ViewNum)
		// Don't return here as we need to process the start view change message
	}

	// Already in view change status and the view number matches
	//
	// Cover 1 state
	// TOTAL: 5
	if r.state.Status == ReplicaStatusViewChange && ev.ViewNum == r.state.ViewNumber {
		_, ok := r.internal.pendingStartViewChange[ev.ViewNum]
		if !ok {
			r.internal.pendingStartViewChange[ev.ViewNum] = pendingStartViewChange{
				Responses: map[uint64]struct{}{},
			}
		}

		r.internal.pendingStartViewChange[ev.ViewNum].Responses[ev.ReplicaID] = struct{}{}

		// Check if we have received view change messages from a majority of the replicas
		if len(r.internal.pendingStartViewChange[ev.ViewNum].Responses) > len(r.state.ClusterMembers)/2 {
			slog.Info("received view change from majority", "view num", ev.ViewNum)

			if err := r.sendDoViewChangeToPrimary(r.state.ClusterMembers[r.potentialPrimary()]); err != nil {
				slog.Error("failed to send doViewChange to primary", "err", err)
			}

			slog.Debug("sent doViewChange to primary", "view num", ev.ViewNum)
		}

		return
	}

	// Cover 1 state
	// TOTAL: 5
	if r.state.Status == ReplicaStatusViewChange && ev.ViewNum > r.state.ViewNumber {
		// What happens if while being in view change state we learn of a new view?
		assert.Assert(r.state.ViewNumber < ev.ViewNum, "view number should be higher")
		return
	}

	assert.Assert(false, "should be unreachable state")
}

func (r *Replica) onDoViewChange(src ipv4port.IPv4Port, ev events.DoViewChange) {
	slog.Debug("received doViewChange", "from", src.String())
	r.internal.viewChangeTimeout = r.time.Now()

	if r.state.Status != ReplicaStatusViewChange {
		slog.Debug("not in state - dropping request", "state", r.state.Status)
		return
	}

	// Don't do primary test here as it could be that we missed the start view change
	// and just getting to know about it now but for sanity ensure that the
	// view number is higher than the current view number.
	if ev.ViewNum < r.state.ViewNumber {
		slog.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	_, ok := r.internal.pendingDoViewChange[ev.ViewNum]
	if !ok {
		r.internal.pendingDoViewChange[ev.ViewNum] = pendingDoViewChange{
			Responses: map[uint64]events.DoViewChange{},
		}
	}

	r.internal.pendingDoViewChange[ev.ViewNum].Responses[ev.ReplicaID] = ev

	// Check if we have received do view change messages from a majority of the replicas
	if len(r.internal.pendingDoViewChange[ev.ViewNum].Responses) > len(r.state.ClusterMembers)/2 {
		slog.Info("received doViewChange from majority", "view num", ev.ViewNum)

		r.finalizeViewChange()

		for mi, member := range r.state.ClusterMembers {
			if r.state.ID == uint64(mi) {
				continue
			}

			slog.Debug("sending startView to backup", "backup addr", member.String())
			if err := r.sendStartViewToBackup(member); err != nil {
				slog.Error("failed to send startView to backup", "err", err, "backup addr", member.String())
			}
		}

		slog.Info("view change complete", "view num", ev.ViewNum)
	}
}

func (r *Replica) onStartView(src ipv4port.IPv4Port, ev events.StartView) {
	slog.Debug("received startView", "from", src.String())

	if ev.ViewNum < r.state.ViewNumber {
		slog.Debug("old view number", "received view num", ev.ViewNum, "current view num", r.state.ViewNumber)
		return
	}

	r.state.Logs = ev.Logs
	r.state.CommitNumber = ev.CommitNum
	r.state.ViewNumber = ev.ViewNum
	r.state.Status = ReplicaStatusNormal

	// Send PrepareOK for all the non committed logs
	for i := ev.CommitNum + 1; i <= ev.OpNum; i++ {
		r.state.ClientTable[ev.Logs.ClientIDAt(int(i))] = clientTableData{
			RequestNumber: ev.Logs.RequestIDAt(int(i)),
		}

		slog.Debug("sending prepareOK to primary", "opNum", i)

		if err := r.sendPrepareOKToPrimary(r.state.ClusterMembers[r.potentialPrimary()], i); err != nil {
			slog.Error("failed to send prepareOK to primary", "err", err, "primary addr", r.state.ClusterMembers[r.potentialPrimary()].String())
		}
	}

	delete(r.internal.pendingStartViewChange, ev.ViewNum)
	delete(r.internal.pendingDoViewChange, ev.ViewNum)

	// The primary will eventually send either Commit message or Prepare which
	// will allow the backup to execute the operations against the state machine
}

func (r *Replica) initiateViewChange(viewnum uint64) {
	slog.Debug("initiating view change", "view num", viewnum)

	r.state.Status = ReplicaStatusViewChange
	r.internal.lastStableViewNumber = r.state.ViewNumber
	r.state.ViewNumber = viewnum
	for _, member := range r.state.ClusterMembers {
		slog.Debug("sending startViewChange to replica", "replica addr", member.String(), "view num", viewnum)
		if err := r.sendStartViewChange(member, viewnum); err != nil {
			slog.Error("failed to send startViewChange to replica", "err", err, "replica addr", member.String())
		}
	}
}

func (r *Replica) performStateTransfer() error {
	panic("unimplemented")
}

func (r *Replica) sendPrepareToBackup(backup ipv4port.IPv4Port, req events.Request) error {
	r.internal.lastPingedBackup[findIDForAddr(r.state.ClusterMembers, backup)] = r.time.Now()
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
	r.internal.lastPingedBackup[findIDForAddr(r.state.ClusterMembers, backup)] = r.time.Now()
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

	r.state.Logs = bestLogs
	r.state.CommitNumber = bestCommit
	r.state.Status = ReplicaStatusNormal

	// Need to seed tables
	for i := r.state.CommitNumber + 1; i <= r.state.OpNum; i++ {
		r.state.ClientTable[r.state.Logs.ClientIDAt(int(i))] = clientTableData{
			RequestNumber: r.state.Logs.RequestIDAt(int(i)),
		}

		r.internal.pendingPOKs[i] = pendingPrepareOK{
			Responses: map[uint64]struct{}{
				r.state.ID: {},
			},
		}
	}
}

func (r *Replica) detectViewChangeNeed() {
	if r.potentialPrimary() != r.state.ID &&
		timedout(r.time.Now(), r.internal.viewChangeTimeout, r.heartbeattimeout) {
		// This will processed next as the entire thing is single threaded
		// nothing can be submitted to the queue while we are processing this
		slog.Debug("primary timed out", "last heard", r.internal.viewChangeTimeout)
		r.internal.sq.Push(events.NetworkEvent{
			Event: &events.Event{
				Type: internalEventType(proposeViewChange),
			},
		})

		r.internal.viewChangeTimeout = r.time.Now()
	}
}

func (r *Replica) potentialPrimary() uint64 {
	return uint64(int(r.state.ViewNumber) % len(r.state.ClusterMembers))
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

func timedout(now, last, timeout uint64) bool {
	return now-last > timeout
}

func findIDForAddr(members []ipv4port.IPv4Port, addr ipv4port.IPv4Port) int {
	for i, v := range members {
		if v.String() == addr.String() {
			return i
		}
	}

	return -1
}
