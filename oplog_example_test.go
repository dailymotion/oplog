package oplog_test

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/dailymotion/oplog"
)

func ExampleOpLog_Append() {
	ol, err := oplog.New("mongodb://localhost/oplog", 1048576)
	if err != nil {
		log.Fatal(err)
	}
	op := oplog.NewOperation("insert", time.Now(), "123", "user", nil)
	ol.Append(op)
}

func ExampleOpLog_Ingest() {
	ol, err := oplog.New("mongodb://localhost/oplog", 1048576)
	if err != nil {
		log.Fatal(err)
	}
	ops := make(chan *oplog.Operation)
	done := make(chan bool, 1)
	go ol.Ingest(ops, nil)
	// Insert a large number of operations
	for i := 0; i < 1000; i++ {
		ops <- oplog.NewOperation("insert", time.Now(), strconv.FormatInt(int64(i), 10), "user", nil)
	}
	done <- true
}

func ExampleOpLog_Tail() {
	ol, err := oplog.New("mongodb://localhost/oplog", 1048576)
	if err != nil {
		log.Fatal(err)
	}
	ops := make(chan oplog.GenericEvent)
	stop := make(chan bool)
	// Tail all future events with no filters
	go ol.Tail(nil, oplog.Filter{}, ops, stop)
	// Read 100 events
	for i := 0; i < 100; i++ {
		op := <-ops
		op.WriteTo(os.Stdout)
	}
	// Stop the tail
	stop <- true
}
