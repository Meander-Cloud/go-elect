package election

import (
	"log"
	"time"

	"github.com/Meander-Cloud/go-schedule/scheduler"

	"github.com/Meander-Cloud/go-elect/arbiter"
	m "github.com/Meander-Cloud/go-elect/message"
	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

// invoked on arbiter goroutine
func (e *Election) councilLockPeer(peerID string, ephemeral bool) {
	e.state.CouncilForPeer = peerID

	log.Printf(
		"%s: role=%s, peerID=%s, ephemeral=%t",
		e.c.LogPrefix,
		e.state.Role,
		peerID,
		ephemeral,
	)

	if ephemeral {
		e.councilScheduleLockWait()
	}
}

// invoked on arbiter goroutine
func (e *Election) councilScheduleLockWait() {
	if e.state.CouncilLockWaitScheduled {
		// no-op
		return
	}

	group := arbiter.GroupCouncilLockWait
	wait := time.Millisecond * time.Duration(e.c.CouncilLockWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{group},
				wait,
				func() {
					// invoked on arbiter goroutine
					e.state.CouncilLockWaitScheduled = false

					if e.state.Role != m.RoleCouncil {
						log.Printf(
							"%s: role=%s mismatch triggered: %s",
							e.c.LogPrefix,
							e.state.Role,
							group,
						)
						return
					}

					log.Printf(
						"%s: role=%s, councilForPeer=%s, wait timeout, participant<%d> lock: %d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.CouncilForPeer,
						len(e.state.PeerMap)+1,
						e.state.QuorumParticipantCount,
						e.state.TotalParticipantCount,
					)

					e.councilToFollower()
				},
				nil,
			),
		},
	)

	e.state.CouncilLockWaitScheduled = true

	log.Printf(
		"%s: role=%s, councilForPeer=%s, scheduled<%v>: %s",
		e.c.LogPrefix,
		e.state.Role,
		e.state.CouncilForPeer,
		wait,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilReleaseLockWait() {
	if !e.state.CouncilLockWaitScheduled {
		// no-op
		return
	}

	group := arbiter.GroupCouncilLockWait

	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[arbiter.Group]{
			Group: group,
		},
	)

	e.state.CouncilLockWaitScheduled = false

	log.Printf(
		"%s: role=%s, councilForPeer=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		e.state.CouncilForPeer,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilToFollower() {
	e.state.CouncilForPeer = ""

	oldRole := e.state.Role
	newRole := m.RoleFollower
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.followerCheckQuorum()
}

// invoked on arbiter goroutine
func (e *Election) councilParticipantInit(connState *tp.ConnState) {
	e.commonParticipantInit(connState)
}

// invoked on arbiter goroutine
func (e *Election) councilParticipantExit(connState *tp.ConnState) {
	cvd := connState.Data.Load()
	e.commonParticipantExit(cvd)

	func() {
		if e.state.CouncilForPeer != cvd.PeerID {
			return
		}

		log.Printf(
			"%s: %s: role=%s, councilForPeer=%s, peer exited, participant<%d> lock: %d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.CouncilForPeer,
			len(e.state.PeerMap)+1,
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		e.councilReleaseLockWait()

		e.councilToFollower()
	}()
}

// invoked on arbiter goroutine
func (e *Election) councilCandidateVoteRequest(p *tp.Client, connState *tp.ConnState, candidateVoteRequest *m.CandidateVoteRequest) {
	var vote uint8 = 0
	reason := m.CandidateVoteReasonRoleCouncil
	defer func() {
		p.WriteSync(
			connState,
			&m.Message{
				Txseq:  p.GetNextTxseq(),
				Txtime: time.Now().UTC().UnixMilli(),

				CandidateVoteResponse: &m.CandidateVoteResponse{
					Term:   candidateVoteRequest.Term,
					Vote:   vote,
					Reason: reason,
				},
			},
		)
	}()

	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, councilForPeer=%s, votedTerm=%d, requestTerm=%d, vote=%d, reason=%s, participant<%d> lock: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.CouncilForPeer,
		e.state.VotedTerm,
		candidateVoteRequest.Term,
		vote,
		reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilCandidateVoteResponse(connState *tp.ConnState, candidateVoteResponse *m.CandidateVoteResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, councilForPeer=%s, no-op vote-response, term=%d, vote=%d, reason=%s, participant<%d> lock: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.CouncilForPeer,
		candidateVoteResponse.Term,
		candidateVoteResponse.Vote,
		candidateVoteResponse.Reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilNomineeAckRequest(p *tp.Client, connState *tp.ConnState, nomineeAckRequest *m.NomineeAckRequest) {
	var ack uint8 = 0
	reason := m.NomineeAckReasonRoleCouncil
	defer func() {
		p.WriteSync(
			connState,
			&m.Message{
				Txseq:  p.GetNextTxseq(),
				Txtime: time.Now().UTC().UnixMilli(),

				NomineeAckResponse: &m.NomineeAckResponse{
					Term:   nomineeAckRequest.Term,
					Ack:    ack,
					Reason: reason,
				},
			},
		)
	}()

	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, councilForPeer=%s, requestTerm=%d, ack=%d, reason=%s, participant<%d> lock: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.CouncilForPeer,
		nomineeAckRequest.Term,
		ack,
		reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilNomineeAckResponse(connState *tp.ConnState, nomineeAckResponse *m.NomineeAckResponse) {
	cvd := connState.Data.Load()
	log.Printf(
		"%s: %s: role=%s, councilForPeer=%s, no-op ack-response, term=%d, ack=%d, reason=%s, participant<%d> lock: %d<%d>",
		e.c.LogPrefix,
		cvd.Descriptor,
		e.state.Role,
		e.state.CouncilForPeer,
		nomineeAckResponse.Term,
		nomineeAckResponse.Ack,
		nomineeAckResponse.Reason,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)
}

// invoked on arbiter goroutine
func (e *Election) councilNomineeRelinquish(connState *tp.ConnState, nomineeRelinquish *m.NomineeRelinquish) {
	cvd := connState.Data.Load()

	func() {
		if e.state.CouncilForPeer != cvd.PeerID {
			log.Printf(
				"%s: %s: role=%s, councilForPeer=%s, no-op nominee-relinquish, term=%d, reason=%s, participant<%d> lock: %d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.CouncilForPeer,
				nomineeRelinquish.Term,
				nomineeRelinquish.Reason,
				len(e.state.PeerMap)+1,
				e.state.QuorumParticipantCount,
				e.state.TotalParticipantCount,
			)
			return
		}

		log.Printf(
			"%s: %s: role=%s, councilForPeer=%s, received nominee-relinquish, term=%d, reason=%s, participant<%d> lock: %d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.CouncilForPeer,
			nomineeRelinquish.Term,
			nomineeRelinquish.Reason,
			len(e.state.PeerMap)+1,
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		e.councilReleaseLockWait()

		e.councilToFollower()
	}()
}

// invoked on arbiter goroutine
func (e *Election) councilAscendantRelinquish(connState *tp.ConnState, ascendantRelinquish *m.AscendantRelinquish) {
	cvd := connState.Data.Load()

	func() {
		if e.state.CouncilForPeer != cvd.PeerID {
			log.Printf(
				"%s: %s: role=%s, councilForPeer=%s, no-op ascendant-relinquish, term=%d, reason=%s, participant<%d> lock: %d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.CouncilForPeer,
				ascendantRelinquish.Term,
				ascendantRelinquish.Reason,
				len(e.state.PeerMap)+1,
				e.state.QuorumParticipantCount,
				e.state.TotalParticipantCount,
			)
			return
		}

		log.Printf(
			"%s: %s: role=%s, councilForPeer=%s, received ascendant-relinquish, term=%d, reason=%s, participant<%d> lock: %d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.CouncilForPeer,
			ascendantRelinquish.Term,
			ascendantRelinquish.Reason,
			len(e.state.PeerMap)+1,
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		e.councilReleaseLockWait()

		e.councilToFollower()
	}()
}

// invoked on arbiter goroutine
func (e *Election) councilLeaderAnnounce(connState *tp.ConnState, leaderAnnounce *m.LeaderAnnounce) {
	cvd := connState.Data.Load()

	if e.state.CouncilForPeer != cvd.PeerID {
		oldForPeer := e.state.CouncilForPeer
		newForPeer := cvd.PeerID
		e.state.CouncilForPeer = newForPeer

		log.Printf(
			"%s: %s: role=%s, councilForPeer=%s -> %s, received leader-announce, term=%d, participant<%d> lock: %d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			oldForPeer,
			newForPeer,
			leaderAnnounce.Term,
			len(e.state.PeerMap)+1,
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)
	} else {
		log.Printf(
			"%s: %s: role=%s, councilForPeer=%s, received leader-announce, term=%d, participant<%d> lock: %d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.CouncilForPeer,
			leaderAnnounce.Term,
			len(e.state.PeerMap)+1,
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)
	}

	e.councilReleaseLockWait()
}

// invoked on arbiter goroutine
func (e *Election) councilLeaderRelinquish(connState *tp.ConnState, leaderRelinquish *m.LeaderRelinquish) {
	cvd := connState.Data.Load()

	func() {
		if e.state.CouncilForPeer != cvd.PeerID {
			log.Printf(
				"%s: %s: role=%s, councilForPeer=%s, no-op leader-relinquish, term=%d, reason=%s, participant<%d> lock: %d<%d>",
				e.c.LogPrefix,
				cvd.Descriptor,
				e.state.Role,
				e.state.CouncilForPeer,
				leaderRelinquish.Term,
				leaderRelinquish.Reason,
				len(e.state.PeerMap)+1,
				e.state.QuorumParticipantCount,
				e.state.TotalParticipantCount,
			)
			return
		}

		log.Printf(
			"%s: %s: role=%s, councilForPeer=%s, received leader-relinquish, term=%d, reason=%s, participant<%d> lock: %d<%d>",
			e.c.LogPrefix,
			cvd.Descriptor,
			e.state.Role,
			e.state.CouncilForPeer,
			leaderRelinquish.Term,
			leaderRelinquish.Reason,
			len(e.state.PeerMap)+1,
			e.state.QuorumParticipantCount,
			e.state.TotalParticipantCount,
		)

		e.councilReleaseLockWait()

		e.councilToFollower()
	}()
}
