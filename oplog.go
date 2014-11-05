package oplog

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type OpLog struct {
	db *mgo.Database
	c  *mgo.Collection
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

type OpHandler func(*Operation) bool

// NewOpLog returns an OpLog connected to the given provided mongo URL.
// If the capped collection does not exists, it will be created with the max
// size defined by maxBytes parameter.
func NewOpLog(mongoURL string, maxBytes int) (*OpLog, error) {
	session, err := mgo.Dial(mongoURL)
	if err != nil {
		return nil, err
	}

	db := session.DB("")
	c := db.C("oplog")

	oplog := &OpLog{db, c}
	oplog.init(maxBytes)
	return oplog, nil
}

// Close closes the underlaying MongoDB session.
func (oplog *OpLog) Close() {
	oplog.db.Session.Close()
}

// init creates capped collection if it does not exists.
func (oplog *OpLog) init(maxBytes int) {
	exists := false
	names, _ := oplog.db.CollectionNames()
	for _, name := range names {
		if name == "oplog" {
			exists = true
			break
		}
	}
	if !exists {
		log.Info("Creating capped collection")
		oplog.c.Create(&mgo.CollectionInfo{
			Capped:   true,
			MaxBytes: maxBytes,
		})
	}
}

// ping tests the MongoDB connection and tries to reconnect in case of error
func (oplog *OpLog) ping() {
	if err := oplog.db.Session.Ping(); err != nil {
		oplog.db.Session.Refresh()
	}
}

// Insert append a operation into the OpLog
func (oplog *OpLog) Insert(operation *Operation) error {
	// TODO: buffering + resume on db down
	return oplog.c.Insert(operation)
}

// HasId checks if an operation id is present in the capped collection.
func (oplog *OpLog) HasId(id bson.ObjectId) (bool, error) {
	count, err := oplog.c.FindId(id).Count()
	if err != nil {
		return false, err
	}
	return count != 0, nil
}

func (oplog *OpLog) LastId() *bson.ObjectId {
	operation := &Operation{}
	oplog.c.Find(nil).Sort("-$natural").One(operation)
	return operation.Id
}

func (oplog *OpLog) tail(lastId *bson.ObjectId) *mgo.Iter {
	var query *mgo.Query
	if lastId == nil {
		// If no last id provided, find the last operation id in the colleciton
		lastId = oplog.LastId()
	}
	if lastId != nil {
		query = oplog.c.Find(bson.M{"_id": bson.M{"$gt": lastId}})
	} else {
		// If last id is still nil, that means the collection is empty
		// Read from the begining
		query = oplog.c.Find(nil)
	}
	return query.Sort("$natural").Tail(5 * time.Second)
}

// Tail tails all the new operations in the oplog and send the operation in
// the given channel. If the lastId parameter is given, all operation posted after
// this event will be returned.
func (oplog *OpLog) Tail(lastId *bson.ObjectId, out chan<- Operation, err chan<- error) {
	operation := Operation{}
	oplog.ping()
	iter := oplog.tail(lastId)

	for {
		for iter.Next(&operation) {
			lastId = operation.Id
			out <- operation
		}

		if iter.Timeout() {
			continue
		}

		if iter.Err() != nil {
			err <- iter.Close()
		}

		iter = oplog.tail(lastId)
	}
}
