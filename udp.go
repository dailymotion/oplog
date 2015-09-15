package oplog

import (
	"net"

	log "github.com/Sirupsen/logrus"
)

// UDPDaemon listens for events and send them to the oplog MongoDB capped collection
type UDPDaemon struct {
	addr string
	ol   *OpLog
}

// NewUDPDaemon create a deamon listening for operations over UDP
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

	daemon.ol.Stats.QueueMaxSize.Set(int64(queueMaxSize))
	ops := make(chan *Operation, queueMaxSize)
	go daemon.ol.Ingest(ops, nil)

	for {
		buffer := make([]byte, 1024)

		n, _, err := c.ReadFromUDP(buffer)
		if err != nil {
			log.Warnf("UDP read error: %s", err)
			continue
		}

		log.Debugf("UDP received operation from UDP: %s", buffer[:n])

		queueSize := len(ops)
		daemon.ol.Stats.QueueSize.Set(int64(queueSize))
		if queueSize >= queueMaxSize {
			// This check is preventive but racy, see select below for a non racy buffer
			// overflow check
			log.Warnf("UDP input queue is full, thowing message: %s", buffer[:n])
			daemon.ol.Stats.EventsDiscarded.Add(1)
			continue
		}

		op, err := decodeOperation(buffer[:n])
		if err != nil {
			log.Warnf("UDP invalid operation received: %s", err)
			daemon.ol.Stats.EventsError.Add(1)
			continue
		}

		// Append to buffered channel in a non-blocking way so we can discard operations
		// if buffer is full.
		select {
		case ops <- op:
			daemon.ol.Stats.EventsReceived.Add(1)
		default:
			log.Warnf("UDP input queue is full, thowing message: %s", buffer[:n])
			daemon.ol.Stats.EventsDiscarded.Add(1)
		}
	}
}
