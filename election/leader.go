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
func (e *Election) leaderEnact() {
	// reaching here implies quorum is intact
	log.Printf(
		"%s: role=%s, selfTerm=%d, participant<%d> quorum: %d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		e.state.SelfTerm,
		len(e.state.PeerMap)+1,
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

				LeaderAnnounce: &m.LeaderAnnounce{
					Term: e.state.SelfTerm,
				},
			},
		)
	}

	e.uc.LeaderElected(
		&LeaderElected{
			Participant: e.state.SelfParticipant.Clone(),
			Time:        time.Now().UTC(),
		},
	)
}

// invoked on arbiter goroutine
func (e *Election) leaderCheckQuorum() {
	participantCount := uint16(len(e.state.PeerMap)) + 1
	log.Printf(
		"%s: role=%s, selfTerm=%d, participant<%d> quorum: %d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		e.state.SelfTerm,
		participantCount,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	if participantCount >= e.state.QuorumParticipantCount {
		e.leaderReleaseQuorumLossWait()
		return
	}

	e.leaderScheduleQuorumLossWait()
}

// invoked on arbiter goroutine
func (e *Election) leaderScheduleQuorumLossWait() {
	if e.state.LeaderQuorumLossWaitScheduled {
		// no-op
		return
	}

	group := arbiter.GroupLeaderQuorumLossWait
	wait := time.Millisecond * time.Duration(e.c.LeaderQuorumLossWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{group},
				wait,
				func() {
					// invoked on arbiter goroutine
					e.state.LeaderQuorumLossWaitScheduled = false

					log.Printf(
						"%s: role=%s, wait timeout, selfTerm=%d, participant<%d> quorum: %d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.SelfTerm,
						len(e.state.PeerMap)+1,
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

								LeaderRelinquish: &m.LeaderRelinquish{
									Term:   e.state.SelfTerm,
									Reason: m.LeaderRelinquishReasonQuorumLoss,
								},
							},
						)
					}

					e.uc.LeaderRevoked(
						&LeaderRevoked{
							Participant: e.state.SelfParticipant.Clone(),
							Time:        time.Now().UTC(),
						},
					)

					e.leaderToFollower()
				},
				nil,
			),
		},
	)

	e.state.LeaderQuorumLossWaitScheduled = true

	log.Printf(
		"%s: role=%s, scheduled<%v>: %s",
		e.c.LogPrefix,
		e.state.Role,
		wait,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) leaderReleaseQuorumLossWait() {
	if !e.state.LeaderQuorumLossWaitScheduled {
		// no-op
		return
	}

	group := arbiter.GroupLeaderQuorumLossWait

	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[arbiter.Group]{
			Group: group,
		},
	)

	e.state.LeaderQuorumLossWaitScheduled = false

	log.Printf(
		"%s: role=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		group,
	)
}

// invoked on arbiter goroutine
func (e *Election) leaderToFollower() {
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
func (e *Election) leaderParticipantInit(p *tp.Server, connState *tp.ConnState) {
	e.commonParticipantInit(connState)

	p.WriteSync(
		connState,
		&m.Message{
			Txseq:  p.GetNextTxseq(),
			Txtime: time.Now().UTC().UnixMilli(),

			LeaderAnnounce: &m.LeaderAnnounce{
				Term: e.state.SelfTerm,
			},
		},
	)

	e.leaderCheckQuorum()
}
