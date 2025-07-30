package election

import (
	"log"
	"time"

	"github.com/Meander-Cloud/go-schedule/scheduler"

	"github.com/Meander-Cloud/go-elect/arbiter"
	m "github.com/Meander-Cloud/go-elect/message"
)

// invoked on arbiter goroutine
func (e *Election) ascendantAssert() {
	// reaching here implies quorum is intact
	log.Printf(
		"%s: role=%s, selfTerm=%d, participant<%d> assert: %d<%d>",
		e.c.LogPrefix,
		e.state.Role,
		e.state.SelfTerm,
		len(e.state.PeerMap)+1,
		e.state.QuorumParticipantCount,
		e.state.TotalParticipantCount,
	)

	e.ascendantScheduleAssertWait()
}

// invoked on arbiter goroutine
func (e *Election) ascendantScheduleAssertWait() {
	wait := time.Millisecond * time.Duration(e.c.AscendantAssertWait)

	e.a.Scheduler().ProcessSync(
		&scheduler.ScheduleAsyncEvent[arbiter.Group]{
			AsyncVariant: scheduler.TimerAsync(
				true,
				[]arbiter.Group{arbiter.GroupAscendantAssertWait},
				wait,
				func() {
					// invoked on arbiter goroutine
					log.Printf(
						"%s: role=%s, wait done, selfTerm=%d, participant<%d> assert: %d<%d>",
						e.c.LogPrefix,
						e.state.Role,
						e.state.SelfTerm,
						len(e.state.PeerMap)+1,
						e.state.QuorumParticipantCount,
						e.state.TotalParticipantCount,
					)

					e.ascendantToLeader()
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
func (e *Election) ascendantReleaseAssertWait() {
	e.a.Scheduler().ProcessSync(
		&scheduler.ReleaseGroupEvent[arbiter.Group]{
			Group: arbiter.GroupAscendantAssertWait,
		},
	)

	log.Printf(
		"%s: role=%s, released: %s",
		e.c.LogPrefix,
		e.state.Role,
		arbiter.GroupAscendantAssertWait,
	)
}

// invoked on arbiter goroutine
func (e *Election) ascendantToFollower() {
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
func (e *Election) ascendantToLeader() {
	// reaching here implies quorum is intact
	oldRole := e.state.Role
	newRole := m.RoleLeader
	e.state.Role = newRole

	log.Printf(
		"%s: role=%s -> %s",
		e.c.LogPrefix,
		oldRole,
		newRole,
	)

	e.leaderEnact()
}
