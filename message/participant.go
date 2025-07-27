package message

type Participant struct {
	Host     string `json:"host"`
	Instance string `json:"instance"`
	Time     int64  `json:"time"` // epoch milliseconds
}

type ParticipantInit struct {
	Participant *Participant `json:"participant"`
	InReconnect bool         `json:"in_reconnect"`
}

type ParticipantExit struct {
	InShutdown bool `json:"in_shutdown"`
}
