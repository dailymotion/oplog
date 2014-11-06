package oplog

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type OpLog struct {
	s *mgo.Session
}

type OpLogFilter struct {
	Type string
}

type OpLogCollection struct {
	*mgo.Collection
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
	UserId    string    `bson:"uid" json:"user_id"`
	Type      string    `bson:"t" json:"type"`
	Id        string    `bson:"id" json:"id"`
}

// objectId returns a bson.ObjectId from an hex representation of an object id of nil
// if an empty string is passed
func objectId(id string) *bson.ObjectId {
	if id != "" {
		oid := bson.ObjectIdHex(id)
		return &oid
	}
	return nil
}

// Close closes the underlaying mgo session
func (c *OpLogCollection) Close() {
	c.Database.Session.Close()
}

// Info returns a human readable version of the operation
func (op *Operation) Info() string {
	id := "(new)"
	if op.Id != nil {
		id = op.Id.Hex()
	}
	return fmt.Sprintf("%s:%s(%s:%s from %s)", id, op.Event, op.Data.Type, op.Data.Id, op.Data.UserId)
}

// NewOpLog returns an OpLog connected to the given provided mongo URL.
// If the capped collection does not exists, it will be created with the max
// size defined by maxBytes parameter.
func NewOpLog(mongoURL string, maxBytes int) (*OpLog, error) {
	session, err := mgo.Dial(mongoURL)
	if err != nil {
		return nil, err
	}
	oplog := &OpLog{session}
	oplog.init(maxBytes)
	return oplog, nil
}

func (oplog *OpLog) c() *OpLogCollection {
	return &OpLogCollection{oplog.s.Copy().DB("").C("oplog")}
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
		oplog.c().Create(&mgo.CollectionInfo{
			Capped:   true,
			MaxBytes: maxBytes,
		})
	}
}

// Insert append a operation into the OpLog
func (oplog *OpLog) Ingest(ops <-chan *Operation) {
	c := oplog.c()
	for {
		select {
		case op := <-ops:
			log.Debugf("OPLOG ingest operation: %#v", op.Info())
			for {
				if err := c.Insert(op); err != nil {
					log.Warnf("OPLOG can't insert operation, try to reconnect: %s", err)
					// Try to reconnect
					time.Sleep(time.Second)
					c.Close()
					c = oplog.c()
					continue
				}
				break
			}
		}
	}
}

// HasId checks if an operation id is present in the capped collection.
func (oplog *OpLog) HasId(id string) bool {
	c := oplog.c()
	defer c.Close()
	oid := objectId(id)
	if oid == nil {
		return false
	}
	count, err := c.FindId(oid).Count()
	if err != nil {
		return false
	}
	return count != 0
}

// LastId returns the most recently inserted operation id if any or "" if oplog is empty
func (oplog *OpLog) LastId() string {
	c := oplog.c()
	defer c.Close()
	operation := &Operation{}
	c.Find(nil).Sort("-$natural").One(operation)
	if operation.Id != nil {
		return operation.Id.Hex()
	}
	return ""
}

// tail creates a tail cursor starting at a given id
func (oplog *OpLog) tail(c *OpLogCollection, lastId *bson.ObjectId, filter OpLogFilter) *mgo.Iter {
	query := bson.M{}
	if filter.Type != "" {
		query["data.t"] = filter.Type
	}
	if lastId == nil {
		// If no last id provided, find the last operation id in the colleciton
		lastId = objectId(oplog.LastId())
	}
	if lastId != nil {
		query["_id"] = bson.M{"$gt": lastId}
	}
	return c.Find(query).Sort("$natural").Tail(5 * time.Second)
}

// Tail tails all the new operations in the oplog and send the operation in
// the given channel. If the lastId parameter is given, all operation posted after
// this event will be returned.
func (oplog *OpLog) Tail(lastId string, filter OpLogFilter, out chan<- Operation, err chan<- error) {
	c := oplog.c()
	operation := Operation{}
	lastObjectId := objectId(lastId)
	iter := oplog.tail(c, lastObjectId, filter)

	for {
		for iter.Next(&operation) {
			lastObjectId = operation.Id
			out <- operation
		}

		if iter.Timeout() {
			continue
		}

		// Try to reconnect
		log.Warnf("OPLOG tail failed with error, try to reconnect: %s", iter.Err())
		iter.Close()
		c.Close()
		c = oplog.c()
		iter = oplog.tail(c, lastObjectId, filter)
		time.Sleep(time.Second)
	}
}
