package election

import (
	"github.com/Meander-Cloud/go-elect/arbiter"
	"github.com/Meander-Cloud/go-elect/config"
)

type Election struct {
	c     *config.Config
	a     *arbiter.Arbiter
	state *State
}

func NewElection(c *config.Config) (*Election, error) {
	err := c.Validate()
	if err != nil {
		return nil, err
	}

	e := &Election{
		c:     c,
		a:     arbiter.NewArbiter(c),
		state: NewState(c),
	}

	return e, nil
}

func (e *Election) Shutdown() {
	e.a.Shutdown() // wait
}
