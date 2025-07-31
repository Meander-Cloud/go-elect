package election

import (
	"time"

	m "github.com/Meander-Cloud/go-elect/message"
)

type LeaderElected struct {
	Participant *m.Participant
	Time        time.Time
}

type LeaderRevoked struct {
	Participant *m.Participant
	Time        time.Time
}

type UserCallback interface {
	LeaderElected(*LeaderElected)
	LeaderRevoked(*LeaderRevoked)
}
