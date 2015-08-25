// The oplogd command is an agent listening on an UDP port for operations and exposing a
// HTTP SSE API.
//
// See README file for more information.
package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/dailymotion/oplog"
)

var (
	debug                = flag.Bool("debug", false, "Show debug log messages.")
	version              = flag.Bool("version", false, "Show oplog version.")
	listenAddr           = flag.String("listen", ":8042", "The address to listen on. Same address is used for both SSE(HTTP) and UDP APIs.")
	mongoURL             = flag.String("mongo-url", os.Getenv("OPLOGD_MONGO_URL"), "MongoDB URL to connect to.")
	cappedCollectionSize = flag.Int("capped-collection-size", 1048576, "Size of the created MongoDB capped collection size in bytes (default 1MB).")
	maxQueuedEvents      = flag.Int("max-queued-events", 100000, "Number of events to queue before starting throwing UDP messages.")
	password             = flag.String("password", os.Getenv("OPLOGD_PASSWORD"), "Password protecting the global SSE stream.")
	objectURL            = flag.String("object-url", os.Getenv("OPLOGD_OBJECT_URL"), "A URL template to reference objects. If this option is set, SSE events will have an \"ref\" field with the URL to the object. The URL should contain {{type}} and {{id}} variables (i.e.: http://api.mydomain.com/{{type}}/{{id}})")
)

// Test
func main() {
	flag.Parse()

	if *version {
		fmt.Println(oplog.VERSION)
		return
	}

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	log.Infof("Starting oplog %s", oplog.VERSION)

	ol, err := oplog.New(*mongoURL, *cappedCollectionSize)
	if err != nil {
		log.Fatal(err)
	}
	ol.ObjectURL = *objectURL

	log.Infof("Listening on %s (UDP/TCP)", *listenAddr)

	udpd := oplog.NewUDPDaemon(*listenAddr, ol)
	go func() {
		log.Fatal(udpd.Run(*maxQueuedEvents))
	}()

	ssed := oplog.NewSSEDaemon(*listenAddr, ol)
	ssed.Password = *password
	log.Fatal(ssed.Run())
}
