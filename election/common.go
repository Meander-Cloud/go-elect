package election

import (
	"log"

	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

// invoked on arbiter goroutine
func (e *Election) commonParticipantInit(connState *tp.ConnState) {
	cvd := connState.Data.Load()
	cached, found := e.state.PeerMap[cvd.PeerID]
	if found {
		log.Printf(
			"%s: %s: overriding existing peer %s",
			e.c.LogPrefix,
			cvd.Descriptor,
			cached.Data.Load().Descriptor,
		)
	}
	e.state.PeerMap[cvd.PeerID] = connState

	log.Printf(
		"%s: %s: peer joined",
		e.c.LogPrefix,
		cvd.Descriptor,
	)
}

// invoked on arbiter goroutine
func (e *Election) commonParticipantExit(cvd *tp.ConnVolatileData) {
	_, found := e.state.PeerMap[cvd.PeerID]
	if !found {
		log.Printf(
			"%s: %s: peer not found",
			e.c.LogPrefix,
			cvd.Descriptor,
		)
		return
	}
	delete(e.state.PeerMap, cvd.PeerID)

	log.Printf(
		"%s: %s: peer exited",
		e.c.LogPrefix,
		cvd.Descriptor,
	)
}
