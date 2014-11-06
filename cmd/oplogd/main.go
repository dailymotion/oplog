package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"
	"github.com/dailymotion/oplog"
)

var (
	mongoURL             = flag.String("mongo-url", "", "MongoDB URL to connect to.")
	cappedCollectionSize = flag.Int("capped-collection-size", 104857600, "Size of the created MongoDB capped collection size in bytes (default 100MB)")
	maxQueuedEvents      = flag.Int("max-queued-events", 100000, "Number of events to queue before starting throwing UDP messages")
)

func main() {
	flag.Parse()
	log.SetLevel(log.DebugLevel)

	ol, err := oplog.NewOpLog(*mongoURL, *cappedCollectionSize)
	if err != nil {
		log.Fatal(err)
	}

	udpd := oplog.NewUDPDaemon(8042, ol)
	go func() {
		log.Fatal(udpd.Run(*maxQueuedEvents))
	}()

	ssed := oplog.NewSSEDaemon(8042, ol)
	log.Fatal(ssed.Run())
}
