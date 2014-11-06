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
//
// The queueSize parameter defines the number of operation that can be queued before
// the UDP server start throwing messages. This is particularly important to handle underlaying
// MongoDB slowdowns or unavalability.
func (daemon *UDPDaemon) Run(queueSize int) error {
	udpAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%d", daemon.port))
	if err != nil {
		return err
	}

	c, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return err
	}

	ops := make(chan *Operation, queueSize)
	go daemon.ol.Ingest(ops)

	operation := &UDPOperation{}
	for {
		buffer := make([]byte, 1024)

		n, _, err := c.ReadFromUDP(buffer)
		if err != nil {
			log.Warnf("UDP read error: %s", err)
			continue
		}

		log.Debugf("UDP received operation from UDP: %s", buffer[:n])

		if len(ops) >= queueSize {
			log.Warnf("UDP input queue is full, thowing message: %s", buffer[:n])
			continue
		}

		err = json.Unmarshal(buffer[:n], operation)
		if err != nil {
			log.Warnf("UDP invalid JSON recieved: %s", err)
			continue
		}

		ops <- &Operation{
			Event: strings.ToUpper(operation.Event),
			Data: &OperationData{
				Timestamp: time.Now(),
				UserId:    operation.UserId,
				Type:      operation.Type,
				Id:        operation.Id,
			},
		}
	}
}
