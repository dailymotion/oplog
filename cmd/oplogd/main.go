package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"
	"github.com/dailymotion/oplog"
)

var (
	listenAddr           = flag.String("listen", ":8042", "The address to listen on. Same address is used for both SSE(HTTP) and UDP APIs.")
	mongoURL             = flag.String("mongo-url", "", "MongoDB URL to connect to.")
	cappedCollectionSize = flag.Int("capped-collection-size", 104857600, "Size of the created MongoDB capped collection size in bytes (default 100MB).")
	maxQueuedEvents      = flag.Int("max-queued-events", 100000, "Number of events to queue before starting throwing UDP messages.")
	password             = flag.String("password", "", "Password protecting the global SSE stream.")
)

func main() {
	flag.Parse()
	log.SetLevel(log.DebugLevel)

	ol, err := oplog.NewOpLog(*mongoURL, *cappedCollectionSize)
	if err != nil {
		log.Fatal(err)
	}

	udpd := oplog.NewUDPDaemon(*listenAddr, ol)
	go func() {
		log.Fatal(udpd.Run(*maxQueuedEvents))
	}()

	ssed := oplog.NewSSEDaemon(*listenAddr, ol)
	ssed.Password = *password
	log.Fatal(ssed.Run())
}
