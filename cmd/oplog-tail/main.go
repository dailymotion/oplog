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
	c, err := consumer.Subscribe(url, consumer.Options{
		StateFile: *stateFile,
		Password:  *password,
		Filter:    f,
	})
	if err != nil {
		log.Fatal(err)
	}

	ops := make(chan *consumer.Operation)
	ack := make(chan *consumer.Operation)
	go c.Process(ops, ack)
	for {
		op := <-ops
		if op.Data == nil {
			fmt.Printf("%s #%s\n", op.Event, op.ID)
		} else {
			fmt.Printf("%s #%s %s/%s\n", op.Event, op.ID, op.Data.Type, op.Data.ID)
			fmt.Printf("  timestamp: %s\n", op.Data.Timestamp)
			fmt.Printf("  parents: %s\n", strings.Join(op.Data.Parents, ", "))
		}
		ack <- op
	}
}
