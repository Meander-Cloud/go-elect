package election

import (
	"log"
	"time"

	m "github.com/Meander-Cloud/go-elect/message"
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
}
