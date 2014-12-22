package oplog

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type OpLog struct {
	s      *mgo.Session
	Status *OpLogStatus
}

type OpLogStatus struct {
	Status string `json:"status"`
	// Total number of events recieved on the UDP interface
	EventsReceived uint64 `json:"events_received"`
	// Total number of events ingested into MongoDB with success
	EventsIngested uint64 `json:"events_ingested"`
	// Total number of events received on the UDP interface with an invalid format
	EventsError uint64 `json:"events_error"`
	// Total number of events discarded because the queue was full
	EventsDiscarded uint64 `json:"events_discarded"`
	// Current number of events in the ingestion queue
	QueueSize uint64 `json:"queue_size"`
	// Maximum number of events allowed in the ingestion queue before discarding events
	QueueMaxSize uint64 `json:"queue_max_size"`
	// Number of clients connected to the SSE API
	Clients int64 `json:"clients"`
}

type OpLogFilter struct {
	Types   []string
	Parents []string
}

// Operation represents an operation stored in the OpLog, ready to be exposed as SSE.
type Operation struct {
	Id    *bson.ObjectId `bson:"_id,omitempty"`
	Event string         `bson:"event"`
	Data  *OperationData `bson:"data"`
}

// OperationData is the data part of the SSE event for the operation.
type OperationData struct {
	Timestamp time.Time `bson:"ts" json:"timestamp"`
	Parents   []string  `bson:"p" json:"parents"`
	Type      string    `bson:"t" json:"type"`
	Id        string    `bson:"id" json:"id"`
}

// ObjectState is the current state of an object given the most recent operation applied on it
type ObjectState struct {
	Id    string         `bson:"_id,omitempty" json:"id"`
	Event string         `bson:"event"`
	Data  *OperationData `bson:"data"`
}

type OperationEvent interface {
	io.WriterTo
	GetEventId() string
}

// parseObjectId returns a bson.ObjectId from an hex representation of an object id or nil
// if an empty string is passed or if the format of the id wasn't valid
func parseObjectId(id string) *bson.ObjectId {
	if id != "" && bson.IsObjectIdHex(id) {
		oid := bson.ObjectIdHex(id)
		return &oid
	}
	return nil
}

// parseTimestampId try to find a millisecond timestamp in the string and return it or return
// false as second value if can be parsed
func parseTimestampId(id string) (ts int64, ok bool) {
	ts = -1
	ok = false
	// Numbers equal or larger than 24 digits are for a timestamp, it is certainly an object id
	if len(id) < 24 {
		if i, err := strconv.ParseInt(id, 10, 64); err == nil {
			ts = i
			ok = true
		}
	}
	return
}

// GetEventId returns an SSE event id as string for the operation
func (op Operation) GetEventId() string {
	return op.Id.Hex()
}

// WriteTo serializes an Operation as a SSE compatible message
func (op Operation) WriteTo(w io.Writer) (int64, error) {
	data, err := json.Marshal(op.Data)
	if err != nil {
		return 0, err
	}
	n, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", op.GetEventId(), op.Event, data)
	return int64(n), err
}

// Info returns a human readable version of the operation
func (op *Operation) Info() string {
	id := "(new)"
	if op.Id != nil {
		id = op.Id.Hex()
	}
	return fmt.Sprintf("%s:%s(%s:%s)", id, op.Event, op.Data.Type, op.Data.Id)
}

// GetEventId returns an SSE event id as string for the object state
func (obj ObjectState) GetEventId() string {
	return strconv.FormatInt(obj.Data.Timestamp.UnixNano()/1000000, 10)
}

// WriteTo serializes an ObjectState as a SSE compatible message
func (obj ObjectState) WriteTo(w io.Writer) (int64, error) {
	data, err := json.Marshal(obj.Data)
	if err != nil {
		return 0, err
	}
	n, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", obj.GetEventId(), obj.Event, data)
	return int64(n), err
}

// NewOpLog returns an OpLog connected to the given provided mongo URL.
// If the capped collection does not exists, it will be created with the max
// size defined by maxBytes parameter.
func NewOpLog(mongoURL string, maxBytes int) (*OpLog, error) {
	session, err := mgo.Dial(mongoURL)
	if err != nil {
		return nil, err
	}
	oplog := &OpLog{session, &OpLogStatus{Status: "OK"}}
	oplog.init(maxBytes)
	return oplog, nil
}

func (oplog *OpLog) DB() *mgo.Database {
	return oplog.s.Copy().DB("")
}

// init creates capped collection if it does not exists.
func (oplog *OpLog) init(maxBytes int) {
	exists := false
	names, _ := oplog.s.DB("").CollectionNames()
	for _, name := range names {
		if name == "oplog" {
			exists = true
			break
		}
	}
	if !exists {
		log.Info("OPLOG creating capped collection")
		oplog.s.DB("").C("oplog").Create(&mgo.CollectionInfo{
			Capped:   true,
			MaxBytes: maxBytes,
		})
	}
}

// Insert append a operation into the OpLog
func (oplog *OpLog) Ingest(ops <-chan *Operation) {
	db := oplog.DB()
	for {
		select {
		case op := <-ops:
			log.Debugf("OPLOG ingest operation: %#v", op.Info())
			atomic.StoreUint64(&oplog.Status.QueueSize, uint64(len(ops)))
			for {
				if err := db.C("oplog").Insert(op); err != nil {
					log.Warnf("OPLOG can't insert operation, try to reconnect: %s", err)
					// Try to reconnect
					time.Sleep(time.Second)
					db.Session.Close()
					db = oplog.DB()
					continue
				}
				break
			}
			// Apply the operation on the state collection
			o := ObjectState{
				Id:    fmt.Sprintf("%s/%s", op.Data.Type, op.Data.Id),
				Event: op.Event,
				Data:  op.Data,
			}
			for {
				if _, err := db.C("objects").Upsert(bson.M{"_id": o.Id}, o); err != nil {
					log.Warnf("OPLOG can't upsert object, try to reconnect: %s", err)
					// Try to reconnect
					time.Sleep(time.Second)
					db.Session.Close()
					db = oplog.DB()
					continue
				}
				break
			}
			atomic.AddUint64(&oplog.Status.EventsIngested, 1)
		}
	}
}

// HasId checks if an operation id is present in the capped collection.
func (oplog *OpLog) HasId(id string) bool {
	db := oplog.DB()
	defer db.Session.Close()

	_, ok := parseTimestampId(id)
	if ok {
		// Id is a timestamp, timestamp are always valid
		return true
	}

	oid := parseObjectId(id)
	if oid == nil {
		return false
	}
	count, err := db.C("oplog").FindId(oid).Count()
	if err != nil {
		return false
	}
	return count != 0
}

// LastId returns the most recently inserted operation id if any or "" if oplog is empty
func (oplog *OpLog) LastId() string {
	db := oplog.DB()
	defer db.Session.Close()
	operation := &Operation{}
	db.C("oplog").Find(nil).Sort("-$natural").One(operation)
	if operation.Id != nil {
		return operation.Id.Hex()
	}
	return ""
}

// tail creates a tail cursor starting at a given id
func (oplog *OpLog) tail(db *mgo.Database, lastId string, filter OpLogFilter) (*mgo.Iter, bool) {
	query := bson.M{}
	if len(filter.Types) > 0 {
		query["data.t"] = bson.M{"$in": filter.Types}
	}
	if len(filter.Parents) > 0 {
		query["data.p"] = bson.M{"$in": filter.Parents}
	}

	log.Debugf("last id: %s", lastId)
	if ts, ok := parseTimestampId(lastId); ok {
		// Id is a timestamp, timestamp are always valid
		query["data.ts"] = bson.M{"$gte": time.Unix(0, ts)}
		return db.C("objects").Find(query).Sort("ts").Iter(), false
	} else {
		oid := parseObjectId(lastId)
		if oid == nil {
			// If no last id provided, find the last operation id in the colleciton
			oid = parseObjectId(oplog.LastId())
		}
		if oid != nil {
			query["_id"] = bson.M{"$gt": oid}
		}
		return db.C("oplog").Find(query).Sort("$natural").Tail(5 * time.Second), true
	}
}

// Tail tails all the new operations in the oplog and send the operation in
// the given channel. If the lastId parameter is given, all operation posted after
// this event will be returned.
//
// If the lastId is a unix timestamp in milliseconds, the tailing will start by syncing
// all the objects last updated after the timestamp.
//
// Giving a lastId of 0 mean syncing all the stored objects before tailing the live updates.
//
// The filter argument can be used to filter on some type of objects or objects with given parrents.
//
// The create, update, delete events are streamed back to the sender thru the out channel with error
// sent thru the err channel.
func (oplog *OpLog) Tail(lastId string, filter OpLogFilter, out chan<- io.WriterTo, err chan<- error) {
	db := oplog.DB()
	operation := Operation{}
	object := ObjectState{}
	var lastEv OperationEvent
	var syncFallbackId string
	iter, tailing := oplog.tail(db, lastId, filter)

	for {
		if tailing {
			log.Debug("OPLOG start tailing")
			for iter.Next(&operation) {
				out <- operation
				lastEv = operation
			}
		} else {
			log.Debug("OPLOG start syncing")
			// Capture the current oplog position in order to resume at this position
			// once full sync is done
			syncFallbackId = oplog.LastId()
			for iter.Next(&object) {
				out <- object
				lastEv = object
			}
		}

		if iter.Timeout() {
			log.Debug("OPLOG timeout")
			continue
		}

		// Try to reconnect
		if tailing {
			log.Warnf("OPLOG tail failed with error, try to reconnect: %s", iter.Err())
		} else if iter.Err() == nil {
			// Syncing done, switch to tailing at the last operation id inserted before
			// the sync was started
			lastId = syncFallbackId
			lastEv = nil
		}
		iter.Close()
		db.Session.Close()
		db = oplog.DB()
		if lastEv != nil {
			lastId = lastEv.GetEventId()
		}
		iter, tailing = oplog.tail(db, lastId, filter)
		time.Sleep(time.Second)
	}
}
