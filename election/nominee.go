package election

import (
	"log"
	"time"

	"github.com/Meander-Cloud/go-schedule/scheduler"

	"github.com/Meander-Cloud/go-elect/arbiter"
	m "github.com/Meander-Cloud/go-elect/message"
)

// invoked on arbiter goroutine
func (e *Election) nomineeRequestAck() {
	// reaching here implies quorum is intact
	ackYesCount := uint16(len(e.state.NomineeAckYesMap)) + 1
	log.Printf(
		"%s: role=%s, selfTerm=%d, participant<%d> ack: %d-%d/%d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		e.state.SelfTerm,
		len(e.state.PeerMap)+1,
		ackYesCount,
		len(e.state.NomineeAckNoMap),
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	if ackYesCount >= e.state.QuorumParticipantCount {
		e.nomineeToAscendant()
		return
	}

	server := e.matrix.Server()
	for _, connState := range e.state.PeerMap {
		server.WriteSync(
			connState,
			&m.Message{
				Txseq:  server.GetNextTxseq(),
				Txtime: time.Now().UTC().UnixMilli(),

				NomineeAckRequest: &m.NomineeAckRequest{
					Term: e.state.SelfTerm,
				},
			},
		)
	}

	e.nomineeScheduleAckWait()
}

// invoked on arbiter goroutine
func (e *Election) nomineeScheduleAckWait() {
	wait := time.Millisecond * time.Duration(e.c.NomineeAckWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{arbiter.GroupNomineeAckWait},
				wait,
				func() {
					// invoked on arbiter goroutine
					log.Printf(
						"%s: role=%s, wait timeout, selfTerm=%d, participant<%d> ack: %d-%d/%d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.SelfTerm,
						len(e.state.PeerMap)+1,
						len(e.state.NomineeAckYesMap)+1,
						len(e.state.NomineeAckNoMap),
						e.state.QuorumParticipantCount,
						e.state.TotalParticipantCount,
					)

					server := e.matrix.Server()
					for _, connState := range e.state.PeerMap {
						server.WriteSync(
							connState,
							&m.Message{
								Txseq:  server.GetNextTxseq(),
								Txtime: time.Now().UTC().UnixMilli(),

								NomineeRelinquish: &m.NomineeRelinquish{
									Term:   e.state.SelfTerm,
									Reason: m.NomineeRelinquishReasonAckTimeout,
								},
							},
						)
					}

					e.nomineeToFollower()
				},
				nil,
			),
		},
	)

	log.Printf(
		"%s: role=%s, scheduled wait for %v",
		e.c.LogPrefix,
		e.state.Role,
		wait,
	)
}

// invoked on arbiter goroutine
func (e *Election) nomineeReleaseAckWait() {
	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[arbiter.Group]{
			Group: arbiter.GroupNomineeAckWait,
		},
	)

	log.Printf(
		"%s: role=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		arbiter.GroupNomineeAckWait,
	)
}

// invoked on arbiter goroutine
func (e *Election) nomineeToFollower() {
	clear(e.state.NomineeAckYesMap)
	clear(e.state.NomineeAckNoMap)

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
func (e *Election) nomineeToAscendant() {
	// reaching here implies quorum is intact
	clear(e.state.NomineeAckYesMap)
	clear(e.state.NomineeAckNoMap)

	oldRole := e.state.Role
	newRole := m.RoleAscendant
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.ascendantAssert()
}
