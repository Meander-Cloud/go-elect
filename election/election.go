package election

import (
	"fmt"
	"log"

	"github.com/Meander-Cloud/go-elect/arbiter"
	"github.com/Meander-Cloud/go-elect/config"
	"github.com/Meander-Cloud/go-elect/net/tcp"
)

type Election struct {
	c      *config.Config
	uc     UserCallback
	a      *arbiter.Arbiter
	state  *State
	matrix *tcp.Matrix
}

func NewElection(
	c *config.Config,
	uc UserCallback,
) (*Election, error) {
	err := c.Validate()
	if err != nil {
		return nil, err
	}

	if uc == nil {
		err = fmt.Errorf("%s: nil UserCallback", c.LogPrefix)
		log.Printf("%s", err.Error())
		return nil, err
	}

	e := &Election{
		c:      c,
		uc:     uc,
		a:      arbiter.NewArbiter(c),
		state:  NewState(c),
		matrix: nil,
	}

	defer func() {
		if err != nil {
			e.Shutdown() // wait
		}
	}()

	h := &MessageHandler{
		e: e,
	}

	e.matrix, err = tcp.NewMatrix(
		c,
		e.a,
		h,
		h,
		e.state.SelfParticipant,
		e.state.SelfID,
	)
	if err != nil {
		return nil, err
	}

	e.a.Dispatch(
		func() {
			// invoked on arbiter goroutine
			e.followerCheckQuorum()
		},
	)

	return e, nil
}

func (e *Election) Shutdown() {
	if e.matrix != nil {
		e.matrix.Shutdown() // wait
	}

	if e.a != nil {
		e.a.Shutdown() // wait
	}
}
