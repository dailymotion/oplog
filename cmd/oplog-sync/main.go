// The oplog-sync command performs a maintaince operation on the oplog database to keep it
// in sync with the source data.
//
// The command takes a dump of the source data as input and compares it with the oplog's data.
// For any discrepency, a related oplog event is sent to rectify the oplog's database and all
// its consumers.
//
// The dump must be in a streamable JSON format. Each line is a JSON object with the same schema
// as of the data part of the SEE API response:
//
// 	{"timestamp":"2014-11-06T03:04:39.041-08:00", "parents": ["user/xl2d"], "type":"video", "id":"x34cd"}
// 	{"timestamp":"2014-12-24T02:03:05.167+01:00", "parents": ["user/xkwek"], "type":"video", "id":"x12ab"}
// 	{"timestamp":"2014-12-24T01:03:05.167Z", "parents": ["user/xkwek"], "type":"video", "id":"x54cd"}
//
// The timestamp must represent the last modification date of the object as an RFC 3339 representation.
//
// The oplog-sync command is used with this dump in order to perform the sync. This command will connect
// to the database, do the comparisons and generate the necessary oplog events to fix the deltas. This
// command does not need an oplogd agent to be running.
//
// BE CAREFUL, any object absent of the dump having a timestamp lower than the most recent timestamp
// present in the dump will be deleted from the oplog.
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

	ol, err := oplog.New(*mongoURL, *cappedCollectionSize)
	if err != nil {
		log.Fatal(err)
	}

	fh, err := os.Open(file)
	if err != nil {
		log.Fatalf("SYNC cannot open dump file: %s", err)
	}
	defer fh.Close()

	lines, err := lineCounter(fh)
	if err != nil {
		log.Fatalf("SYNC error counting lines: %s", err)
	}
	fh.Seek(0, 0)
	createMap := make(oplog.OperationDataMap, lines)
	updateMap := make(oplog.OperationDataMap)
	deleteMap := make(oplog.OperationDataMap)

	// Load dump in memory
	obd := oplog.OperationData{}
	scanner := bufio.NewScanner(fh)
	line := 0
	for scanner.Scan() {
		line++
		if err := json.Unmarshal(scanner.Bytes(), &obd); err != nil {
			log.Fatalf("SYNC dump unmarshaling error at line %d: %s", line, err)
		}
		if err := obd.Validate(); err != nil {
			log.Fatalf("SYNC invalid operation at line %d: %s", line, err)
		}
		createMap[obd.GetId()] = obd
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("SYNC dump reading error: %s", err)
	}

	// Scan the oplog db and generate the diff
	if err := ol.Diff(createMap, updateMap, deleteMap); err != nil {
		log.Fatalf("SYNC diff error: %s", err)
	}

	// Generate events to fix the delta
	db := ol.DB()
	defer db.Session.Close()
	op := &oplog.Operation{Event: "create"}
	genEvents := func(opMap oplog.OperationDataMap) {
		for _, obd := range opMap {
			op.Data = &obd
			ol.Append(op, db)
		}
	}
	genEvents(createMap)
	op.Event = "update"
	genEvents(updateMap)
	op.Event = "delete"
	genEvents(deleteMap)
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
