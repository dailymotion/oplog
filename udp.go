package oplog

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

type UDPOperation struct {
	Event  string `json:"event"`
	UserId string `json:"user_id"`
	Type   string `json:"type"`
	Id     string `json:"id"`
}

// UDPDaemon listens for events and send them to the oplog MongoDB capped collection
type UDPDaemon struct {
	port int
	ol   *OpLog
}

func NewUDPDaemon(port int, ol *OpLog) *UDPDaemon {
	return &UDPDaemon{port, ol}
}

// Run reads every datagrams and send them to the oplog
func (daemon *UDPDaemon) Run() error {
	udpAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%d", daemon.port))
	if err != nil {
		return err
	}

	c, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return err
	}

	operation := &UDPOperation{}
	for {
		buffer := make([]byte, 1024)

		n, _, err := c.ReadFromUDP(buffer)
		if err != nil {
			log.Warnf("UDP read error: %s", err)
			continue
		}

		err = json.Unmarshal(buffer[:n], operation)
		if err != nil {
			log.Warnf("Invalid JSON: %s", err)
			continue
		}

		log.Debugf("Received operation from UDP: %#v", operation)
		daemon.ol.Insert(&Operation{
			Event: strings.ToUpper(operation.Event),
			Data: &OperationData{
				Timestamp: time.Now(),
				UserId:    operation.UserId,
				Type:      operation.Type,
				Id:        operation.Id,
			},
		})
	}
}
