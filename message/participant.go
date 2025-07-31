package message

type Participant struct {
	Host     string `json:"host"`
	Instance string `json:"instance"`
	Time     int64  `json:"time"` // epoch milliseconds
}

func (p *Participant) Clone() *Participant {
	return &Participant{
		Host:     p.Host,
		Instance: p.Instance,
		Time:     p.Time,
	}
}

type ParticipantInit struct {
	Participant *Participant `json:"participant"`
	InReconnect bool         `json:"in_reconnect"`
}

type ParticipantExit struct {
	InShutdown bool `json:"in_shutdown"`
}
