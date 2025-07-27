package config

import (
	"fmt"
	"log"
	"time"
)

const (
	// defaults for when not provided in Config
	EventChannelLength   uint16        = 1024
	TcpKeepAliveInterval time.Duration = time.Second * 17
	TcpKeepAliveCount    uint16        = 2
	TcpDialTimeout       time.Duration = time.Second * 3
	TcpReconnectInterval time.Duration = time.Second * 5
	TcpReconnectWindow   time.Duration = time.Second * 17
)

type Config struct {
	Host               string
	Instance           string
	EventChannelLength uint16

	SelfAddress            string
	ParticipantAddressList []string
	TcpKeepAliveInterval   uint16
	TcpKeepAliveCount      uint16
	TcpDialTimeout         uint16
	TcpReconnectInterval   uint16
	TcpReconnectWindow     uint16

	LogPrefix string
	LogDebug  bool
}

func (c *Config) Validate() error {
	if c == nil {
		err := fmt.Errorf("nil config")
		log.Printf("%s", err.Error())
		return err
	}

	if c.Host == "" {
		err := fmt.Errorf("invalid Host=%s", c.Host)
		log.Printf("%s", err.Error())
		return err
	}

	if c.Instance == "" {
		err := fmt.Errorf("invalid Instance=%s", c.Instance)
		log.Printf("%s", err.Error())
		return err
	}

	if c.SelfAddress == "" {
		err := fmt.Errorf("invalid SelfAddress=%s", c.SelfAddress)
		log.Printf("%s", err.Error())
		return err
	}

	palLen := len(c.ParticipantAddressList)
	if palLen == 0 {
		err := fmt.Errorf("empty ParticipantAddressList")
		log.Printf("%s", err.Error())
		return err
	} else if palLen%2 == 0 {
		err := fmt.Errorf("palLen=%d, election must have odd number of participants", palLen)
		log.Printf("%s", err.Error())
		return err
	}

	for _, address := range c.ParticipantAddressList {
		if address == "" {
			err := fmt.Errorf("invalid ParticipantAddressList=%+v", c.ParticipantAddressList)
			log.Printf("%s", err.Error())
			return err
		}
	}

	if c.TcpKeepAliveInterval == 0 {
		err := fmt.Errorf("invalid TcpKeepAliveInterval=%d", c.TcpKeepAliveInterval)
		log.Printf("%s", err.Error())
		return err
	}

	if c.TcpKeepAliveCount == 0 {
		err := fmt.Errorf("invalid TcpKeepAliveCount=%d", c.TcpKeepAliveCount)
		log.Printf("%s", err.Error())
		return err
	}

	if c.TcpDialTimeout == 0 {
		err := fmt.Errorf("invalid TcpDialTimeout=%d", c.TcpDialTimeout)
		log.Printf("%s", err.Error())
		return err
	}

	if c.TcpReconnectInterval == 0 {
		err := fmt.Errorf("invalid TcpReconnectInterval=%d", c.TcpReconnectInterval)
		log.Printf("%s", err.Error())
		return err
	}

	if c.TcpReconnectWindow == 0 {
		err := fmt.Errorf("invalid TcpReconnectWindow=%d", c.TcpReconnectWindow)
		log.Printf("%s", err.Error())
		return err
	}

	return nil
}
