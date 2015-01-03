package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dailymotion/oplog/consumer"
)

var (
	password  = flag.String("password", "", "Password to access the oplog.")
	stateFile = flag.String("state-file", "", "Path to the state file storing the oplog position id (default: no store)")
	types     = flag.String("types", "", "Comma seperated list of types to filter on")
	parents   = flag.String("parents", "", "Comma seperated list of parents type/id to filter on")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Print("  <oplog url>\n")
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	url := flag.Arg(0)

	f := consumer.Filter{
		Types:   strings.Split(*types, ","),
		Parents: strings.Split(*parents, ","),
	}
	c := consumer.Subscribe(url, consumer.Options{
		StateFile: *stateFile,
		Password:  *password,
		Filter:    f,
	})

	ops := make(chan consumer.Operation)
	errs := make(chan error)
	stop := make(chan bool)
	done := make(chan bool)
	go c.Process(ops, errs, stop, done)
	for {
		select {
		case op := <-ops:
			if op.Data == nil {
				fmt.Printf("%s #%s\n", op.Event, op.ID)
			} else {
				fmt.Printf("%s: %s #%s %s/%s (%s)\n",
					op.Data.Timestamp, op.Event, op.ID, op.Data.Type, op.Data.ID, strings.Join(op.Data.Parents, ", "))
			}
			op.Done()
		case err := <-errs:
			switch err {
			case consumer.ErrAccessDenied, consumer.ErrWritingState:
				stop <- true
				log.Fatal(err)
			case consumer.ErrResumeFailed:
				if *stateFile != "" {
					log.Print("Resume failed, forcing full replication")
					c.SetLastId("0")
				} else {
					log.Print(err)
				}
			default:
				log.Print(err)
			}
		case <-done:
			return
		}
	}
}
