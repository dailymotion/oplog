package oplog

import (
	"encoding/json"
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
	addr string
	ol   *OpLog
}

func NewUDPDaemon(addr string, ol *OpLog) *UDPDaemon {
	return &UDPDaemon{addr, ol}
}

// Run reads every datagrams and send them to the oplog
//
// The queueSize parameter defines the number of operation that can be queued before
// the UDP server start throwing messages. This is particularly important to handle underlaying
// MongoDB slowdowns or unavalability.
func (daemon *UDPDaemon) Run(queueMaxSize int) error {
	udpAddr, err := net.ResolveUDPAddr("udp4", daemon.addr)
	if err != nil {
		return err
	}

	c, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return err
	}

	daemon.ol.Status.QueueMaxSize = queueMaxSize
	ops := make(chan *Operation, queueMaxSize)
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

		daemon.ol.Status.QueueSize = len(ops)
		if daemon.ol.Status.QueueSize >= queueMaxSize {
			log.Warnf("UDP input queue is full, thowing message: %s", buffer[:n])
			daemon.ol.Status.EventsDiscarded++
			continue
		}

		err = json.Unmarshal(buffer[:n], operation)
		if err != nil {
			log.Warnf("UDP invalid JSON recieved: %s", err)
			daemon.ol.Status.EventsError++
			continue
		}

		daemon.ol.Status.EventsReceived++
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
