package election

import (
	tp "github.com/Meander-Cloud/go-elect/net/tcp/protocol"
)

// invoked on arbiter goroutine
func (e *Election) councilParticipantInit(connState *tp.ConnState) {
	e.commonParticipantInit(connState)
}

// invoked on arbiter goroutine
func (e *Election) councilParticipantExit(connState *tp.ConnState) {
	// check if exited peer is council target
}
