// Package oplog provides a generic oplog/replication system for REST APIs.
//
// Most of the time, the oplog service is used thru the oplogd agent which uses this
// package. But in the case your application is written in Go, you may want to integrate
// at the code level.
package oplog

import (
	"io"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type OpLog struct {
	s     *mgo.Session
	Stats *Stats
	// ObjectURL is a template URL to be used to generate reference URL to operation's objects.
	// The URL can use {{type}} and {{id}} template as follow: http://api.mydomain.com/{{type}}/{{id}}.
	// If not provided, no "ref" field will be included in oplog events.
	ObjectURL string
	// Number of object to fetch from the states collection per page.
	PageSize int
}

type OperationDataMap map[string]OperationData

// New returns an OpLog connected to the given provided mongo URL.
// If the capped collection does not exists, it will be created with the max
// size defined by maxBytes parameter.
func New(mongoURL string, maxBytes int) (*OpLog, error) {
	session, err := mgo.Dial(mongoURL)
	if err != nil {
		return nil, err
	}
	session.SetSyncTimeout(10 * time.Second)
	session.SetSocketTimeout(20 * time.Second)
	session.SetSafe(&mgo.Safe{})
	stats := NewStats()
	oplog := &OpLog{
		s:        session,
		Stats:    &stats,
		PageSize: 1000,
	}
	oplog.init(maxBytes)
	// Setting monotonic before collection fails with a "not master" error
	session.SetMode(mgo.Monotonic, true)
	return oplog, nil
}

// DB returns the Mongo database object used by the oplog
func (oplog *OpLog) DB() *mgo.Database {
	return oplog.s.Copy().DB("")
}

// init creates capped collection if it does not exists.
func (oplog *OpLog) init(maxBytes int) {
	oplogExists := false
	objectsExists := false
	names, _ := oplog.s.DB("").CollectionNames()
	for _, name := range names {
		switch name {
		case "oplog_ops":
			oplogExists = true
		case "oplog_states":
			objectsExists = true
		}
	}
	if !oplogExists {
		log.Info("OPLOG creating capped collection")
		err := oplog.s.DB("").C("oplog_ops").Create(&mgo.CollectionInfo{
			Capped:   true,
			MaxBytes: maxBytes,
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	if !objectsExists {
		log.Info("OPLOG creating objects index")
		c := oplog.s.DB("").C("oplog_states")
		// Replication query
		if err := c.EnsureIndexKey("event", "ts"); err != nil {
			log.Fatal(err)
		}
		// Replication query with a filter on types
		if err := c.EnsureIndexKey("event", "data.t", "ts"); err != nil {
			log.Fatal(err)
		}
		// Fallback query
		if err := c.EnsureIndexKey("ts"); err != nil {
			log.Fatal(err)
		}
		// Fallback query with a filter on types
		if err := c.EnsureIndexKey("data.t", "ts"); err != nil {
			log.Fatal(err)
		}
	}
}

// Ingest appends an operation into the OpLog thru a channel
func (oplog *OpLog) Ingest(ops <-chan *Operation) {
	db := oplog.DB()
	defer db.Session.Close()
	for {
		select {
		case op := <-ops:
			oplog.Stats.QueueSize.Set(int64(len(ops)))
			oplog.Append(op, db)
		}
	}
}

// Append appends an operation into the OpLog
//
// If the db parameter is not nil, the passed db connection is used. In case of
// error, the db pointer may be replaced by a new alive session.
func (oplog *OpLog) Append(op *Operation, db *mgo.Database) {
	if db == nil {
		db = oplog.DB()
		defer db.Session.Close()
	}
	log.Debugf("OPLOG ingest operation: %#v", op.Info())
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0 // Retry forever
	b.Reset()
	for {
		if err := db.C("oplog_ops").Insert(op); err != nil {
			log.Warnf("OPLOG can't insert operation, retrying: %s", err)
			// Retry with backoff
			time.Sleep(b.NextBackOff())
			db.Session.Refresh()
			continue
		}
		break
	}
	// Apply the operation on the state collection
	event := op.Event
	if event == "update" {
		// Only store insert and delete events in the object stats collection as
		// only the final stat of the object is stored.
		event = "insert"
	}
	o := ObjectState{
		Id:        op.Data.GetId(),
		Event:     event,
		Timestamp: time.Now(),
		Data:      op.Data,
	}
	b.Reset()
	for {
		if _, err := db.C("oplog_states").Upsert(bson.M{"_id": o.Id}, o); err != nil {
			log.Warnf("OPLOG can't upsert object, retrying: %s", err)
			// Retry with backoff
			time.Sleep(b.NextBackOff())
			db.Session.Refresh()
			continue
		}
		break
	}
	oplog.Stats.EventsIngested.Add(1)
}

// Diff finds which objects must be created or deleted in order to fix the delta
//
// The createMap is a map pointing to all objects present in the source database.
// The function search of differences between the passed map and the oplog database and
// remove objects identical in both sides from the createMap and populate the deleteMap
// with objects that are present in the oplog database but not in the source database.
// If an object is present in both createMap and the oplog database but timestamp of the
// oplog object is earlier than createMap's, the object is added to the updateMap.
func (oplog *OpLog) Diff(createMap OperationDataMap, updateMap OperationDataMap, deleteMap OperationDataMap) error {
	db := oplog.DB()
	defer db.Session.Close()

	// Find the most recent timestamp
	dumpTime := time.Unix(0, 0)
	for _, obd := range createMap {
		if obd.Timestamp.After(dumpTime) {
			dumpTime = obd.Timestamp
		}
	}

	obs := ObjectState{}
	iter := db.C("oplog_states").Find(bson.M{}).Iter()
	for iter.Next(&obs) {
		if obs.Event == "deleted" {
			if obd, ok := createMap[obs.Id]; ok {
				// If the object is present in the dump but deleted in the oplog, it means
				// that it has been deleted between the dump creation and the sync
				// (if the oplog version is more recent)
				if obd.Timestamp.Before(obs.Data.Timestamp) {
					delete(createMap, obs.Id)
				}
			}
		} else {
			if obd, ok := createMap[obs.Id]; ok {
				// Object exists on both sides, remove it from the create map
				delete(createMap, obs.Id)
				// If the dump object is newer than oplog's, add it to the update map
				if obs.Data.Timestamp.Before(obd.Timestamp) {
					updateMap[obs.Id] = obd
				}
			} else {
				// The object only exists in the oplog db, add it to the delete map
				// if the timestamp of the found object is older than oldest object
				// in the dump in order to ensure we don't delete an object which
				// have been created between the dump creation and the sync.
				if obs.Data.Timestamp.Before(dumpTime) {
					deleteMap[obs.Id] = *obs.Data
					delete(createMap, obs.Id)
				}
			}
		}
	}
	if iter.Err() != nil {
		return iter.Err()
	}

	return nil
}

// HasId checks if an operation id is present in the capped collection.
func (oplog *OpLog) HasId(id LastId) (bool, error) {
	if olid, ok := id.(*OperationLastId); ok {
		db := oplog.DB()
		defer db.Session.Close()
		count, err := db.C("oplog_ops").FindId(olid.ObjectId).Count()
		return count != 0, err
	}

	// Replication id are always found as they are timestamps
	return true, nil
}

// LastId returns the most recently inserted operation id if any or nil if oplog is empty
func (oplog *OpLog) LastId() (LastId, error) {
	db := oplog.DB()
	defer db.Session.Close()
	operation := &Operation{}
	err := db.C("oplog_ops").Find(nil).Sort("-$natural").One(operation)
	if err == mgo.ErrNotFound {
		return nil, nil
	}
	if operation.Id != nil {
		return &OperationLastId{operation.Id}, nil
	}
	return nil, err
}

// Tail tails all the new operations in the oplog and send the operation in
// the given channel. If the lastId parameter is given, all operation posted after
// this event will be returned.
//
// If the lastId is a ReplicationLastId (unix timestamp in milliseconds), the tailing will
// start by replicating all the objects last updated after the timestamp.
//
// Giving a lastId of 0 mean replicating all the stored objects before tailing the live updates.
//
// The filter argument can be used to filter on some type of objects or objects with given parrents.
//
// The create, update, delete events are streamed back to the sender thru the out channel
func (oplog *OpLog) Tail(lastId LastId, filter OpLogFilter, out chan<- io.WriterTo, stop <-chan bool) {
	var lastEv GenericEvent

	if lastId != nil {
		if r, ok := lastId.(*ReplicationLastId); ok && r.int64 == 0 {
			// When full replication is requested, start by sending a "reset" event to instruct
			// the consumer to reset its database before processing further operations.
			// The id is 1 so if connection is lost after this event and consumer processed the event,
			// the connection recover won't trigger a second "reset" event.
			out <- &OpLogEvent{
				Id:    "1",
				Event: "reset",
			}
		}
	}

	done := false
	mu := &sync.RWMutex{}
	isDone := func() bool {
		mu.RLock()
		defer mu.RUnlock()
		return done
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		db := oplog.DB()
		defer db.Session.Close()

		var iter *mgo.Iter
		defer func() {
			if iter != nil {
				iter.Close()
			}
		}()

		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = 0 // Retry forever
		b.Reset()

		var replicationFallbackId LastId

		for {
			var err error

			if i, ok := lastId.(*OperationLastId); ok {
				log.Debug("OPLOG start live updates")

				query := bson.M{}
				filter.apply(&query)
				if i != nil {
					// Resuming at given last id
					query["_id"] = bson.M{"$gt": i.ObjectId}
				}
				iter = db.C("oplog_ops").Find(query).Sort("$natural").Tail(5 * time.Second)

				operation := Operation{}
				for {
					for iter.Next(&operation) {
						if isDone() {
							return
						}
						if oplog.ObjectURL != "" {
							// If object URL template is provided, generate it from operation's data
							operation.Data.genRef(oplog.ObjectURL)
						}
						out <- operation
						// Save current event for resume
						lastEv = operation
					}

					if iter.Timeout() {
						// On tail timeout, just wait again
						continue
					}
					break
				}

				if isDone() {
					return
				}

				if iter.Err() != nil {
					log.Warnf("OPLOG tail failed with error, try to reconnect: %s", iter.Err())
				} else if operation.Id == nil {
					// This mostly happen when the tail cursor is on an empty collection
					log.Debug("OPLOG ops collection is empty, retrying")
					time.Sleep(b.NextBackOff())
					continue
				} else {
					// Reset the backoff counter
					b.Reset()
				}
			} else if i, ok := lastId.(*ReplicationLastId); ok {
				log.Debug("OPLOG start replication")

				// Capture the current oplog position in order to resume at this position
				// once replication or fallback is done. This also serves a upper limit for
				// the fetching of the data.
				if replicationFallbackId, err = oplog.LastId(); err != nil {
					log.Warnf("OPLOG error retriving replication fallback id: %s", err)
					goto retry
				}

				query := bson.M{}
				filter.apply(&query)
				tsClause := bson.M{}
				query["ts"] = tsClause
				if i.int64 > 0 {
					// Id is a timestamp, timestamp are always valid
					tsClause["$gte"] = i.Time()
				}
				if replicationFallbackId != nil {
					// Do not fetch any new object modified after the current most recent operation
					tsClause["$lte"] = replicationFallbackId.Time()
				}
				if !i.fallbackMode {
					// In replication mode, do only notify about inserts
					// In fallback mode (when operation id is no longer in the capped collection),
					// we must not filter deletes otherwise the consumer will get out of sync
					query["event"] = "insert"
				}

				for {
					// Iterate over the collection using "page" of 1000 items so we don't hold a read lock
					// on the db for too long when the states collection is large or the reader is slow
					iter = db.C("oplog_states").Find(query).Sort("ts").Limit(oplog.PageSize).Iter()

					c := 0
					object := ObjectState{}
					for iter.Next(&object) {
						if isDone() {
							return
						}
						if oplog.ObjectURL != "" {
							object.Data.genRef(oplog.ObjectURL)
						}
						out <- object
						// Save current event for resume
						lastEv = object
						c++
					}

					if isDone() {
						return
					}

					if iter.Err() != nil {
						log.Warnf("OPLOG replication failed with error, retrying: %s", iter.Err())
						goto retry
					}

					if lastEv != nil && c == oplog.PageSize {
						// We consumed on page of event, go to the next page
						tsClause["$gte"] = lastEv.GetEventId().Time()
						continue
					}

					// When the number of returned item is lower than page size, we can assume we where
					// on the last "page".
					break
				}

				// Replication is done, notify and swtich to live event stream
				//
				// Send a "live" operation to inform the consumer it is no live event stream.
				// We use the last event id here in order to ensure the consumer will resume
				// the replication starting at this point in time in case of a failure after
				// the "live" event.
				liveId := "" // default value
				if lastEv != nil {
					liveId = lastEv.GetEventId().String()
				}
				out <- &OpLogEvent{
					Id:    liveId,
					Event: "live",
				}
				// Switch to live update at the last operation id inserted before the replication
				// was started
				lastId = replicationFallbackId
				replicationFallbackId = nil
				lastEv = nil

				// Reset the backoff counter
				b.Reset()
			} else {
				panic("Invalid last id type")
			}

		retry:
			// Prepare for retry with backoff
			iter.Close()
			time.Sleep(b.NextBackOff())
			db.Session.Refresh()
			if lastEv != nil {
				lastId = lastEv.GetEventId()
			}
		}
	}()

	select {
	case <-stop:
		mu.Lock()
		done = true
		mu.Unlock()
		wg.Wait()
		log.Info("OPLOG tail closed")
	}
}
