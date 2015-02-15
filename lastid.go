package oplog

import (
	"errors"
	"strconv"

	"gopkg.in/mgo.v2/bson"
)

type LastId interface {
	// String returns the string representation of their value
	String() string
}

type OperationLastId struct {
	*bson.ObjectId
}

type ReplicationLastId struct {
	int64
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
	if len(id) <= 13 {
		if i, err := strconv.ParseInt(id, 10, 64); err == nil {
			ts = i
			ok = true
		}
	}
	return
}

func NewLastId(id string) (LastId, error) {
	if ts, ok := parseTimestampId(id); ok {
		// Id is a timestamp, timestamp are always valid
		return &ReplicationLastId{ts}, nil
	}

	oid := parseObjectId(id)
	if oid == nil {
		return nil, errors.New("Invalid last id")
	}
	return &OperationLastId{oid}, nil
}

func (rid ReplicationLastId) String() string {
	return strconv.FormatInt(rid.int64, 10)
}

func (oid OperationLastId) String() string {
	return oid.ObjectId.Hex()
}

// Fallback tries to convert a "event" id into a "replication" id by extracting
// the timestamp part of the Mongo ObjectId. If the id is not a valid ObjectId,
// an error is returned.
func (oid *OperationLastId) Fallback() LastId {
	return &ReplicationLastId{oid.Time().UnixNano() / 1000000}
}
