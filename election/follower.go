package election

import (
	"log"

	"github.com/Meander-Cloud/go-schedule/scheduler"

	"github.com/Meander-Cloud/go-elect/arbiter"
	m "github.com/Meander-Cloud/go-elect/message"
)

// invoked on arbiter goroutine
func (e *Election) followerCheckQuorum() {
	participantCount := uint16(len(e.state.PeerMap)) + 1
	log.Printf(
		"%s: role=%s, participant<%d> quorum: %d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		participantCount,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	if participantCount < e.state.QuorumParticipantCount {
		e.followerReleaseWait()
		return
	}

	e.followerScheduleWait()
}

// invoked on arbiter goroutine
func (e *Election) followerScheduleWait() {
	if e.state.FollowerWaitScheduled {
		// no-op
		return
	}

	wait := e.state.GenerateFollowerWait()

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{arbiter.GroupFollowerWait},
				wait,
				func() {
					// invoked on arbiter goroutine
					e.state.FollowerWaitScheduled = false

					e.followerToCandidate()
				},
				nil,
			),
		},
	)

	e.state.FollowerWaitScheduled = true

	log.Printf(
		"%s: role=%s, scheduled wait for %v",
		e.c.LogPrefix,
		e.state.Role,
		wait,
	)
}

// invoked on arbiter goroutine
func (e *Election) followerReleaseWait() {
	if !e.state.FollowerWaitScheduled {
		// no-op
		return
	}

	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[arbiter.Group]{
			Group: arbiter.GroupFollowerWait,
		},
	)

	e.state.FollowerWaitScheduled = false

	log.Printf(
		"%s: role=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		arbiter.GroupFollowerWait,
	)
}

// invoked on arbiter goroutine
func (e *Election) followerToCandidate() {
	// reaching here implies quorum is intact
	oldRole := e.state.Role
	newRole := m.RoleCandidate
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.candidateRequestVote()
}
