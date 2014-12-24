package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/dailymotion/oplog"
)

var (
	debug                = flag.Bool("debug", false, "Show debug log messages.")
	mongoURL             = flag.String("mongo-url", "", "MongoDB URL to connect to.")
	cappedCollectionSize = flag.Int("capped-collection-size", 104857600, "Size of the created MongoDB capped collection size in bytes (default 100MB).")
	maxQueuedEvents      = flag.Uint64("max-queued-events", 100000, "Number of events to queue before starting throwing UDP messages.")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Print("  <dump file>\n")
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	file := flag.Arg(0)

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	ol, err := oplog.NewOpLog(*mongoURL, *cappedCollectionSize)
	if err != nil {
		log.Fatal(err)
	}

	fh, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer fh.Close()

	lines, err := lineCounter(fh)
	fh.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}
	createMap := make(oplog.OperationDataMap, lines)
	deleteMap := make(oplog.OperationDataMap)

	// Load dump in memory
	obd := oplog.OperationData{}
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		json.Unmarshal(scanner.Bytes(), &obd)
		createMap[obd.GetId()] = obd
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Scan the oplog db and generate the diff
	ol.Diff(createMap, deleteMap)

	// Generate events to fix the delta
	db := ol.DB()
	op := &oplog.Operation{Event: "create"}
	for _, obd := range createMap {
		op.Data = &obd
		ol.Append(op, db)
	}
	op.Event = "delete"
	for _, obd := range deleteMap {
		op.Data = &obd
		ol.Append(op, db)
	}
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 8196)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return count, err
		}

		count += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return count, nil
}
