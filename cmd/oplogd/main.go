package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/dailymotion/oplog"
)

func main() {
	log.SetLevel(log.DebugLevel)

	ol, err := oplog.NewOpLog("", 104857600) // 100 MB
	if err != nil {
		log.Fatal(err)
	}
	defer ol.Close()

	udpd := oplog.NewUDPDaemon(8042, ol)
	go func() {
		log.Fatal(udpd.Run())
	}()

	ssed := oplog.NewSSEDaemon(8042, ol)
	log.Fatal(ssed.Run())
}
