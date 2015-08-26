package oplog

import (
	"errors"
	"strconv"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// LastID defines an interface for different kinds of oplog id representations
type LastID interface {
	// String returns the string representation of their value
	String() string
	// Time returns the embedded time
	Time() time.Time
}

// OperationLastID represents an actual stored operation id
type OperationLastID struct {
	*bson.ObjectId
}

// ReplicationLastID represents a timestamp id allowing to hook into operation feed by time
type ReplicationLastID struct {
	int64
	fallbackMode bool
}

// parseObjectID returns a bson.ObjectId from an hex representation of an object id or nil
// if an empty string is passed or if the format of the id wasn't valid
func parseObjectID(id string) *bson.ObjectId {
	if id != "" && bson.IsObjectIdHex(id) {
		oid := bson.ObjectIdHex(id)
		return &oid
	}
	return nil
}

// parseTimestampID try to find a millisecond timestamp in the string and return it or return
// false as second value if can be parsed
func parseTimestampID(id string) (ts int64, ok bool) {
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

// NewLastID creates a last id from a string containing either a operation id
// or a replication id.
func NewLastID(id string) (LastID, error) {
	if ts, ok := parseTimestampID(id); ok {
		// Id is a timestamp, timestamp are always valid
		return &ReplicationLastID{ts, false}, nil
	}

	oid := parseObjectID(id)
	if oid == nil {
		return nil, errors.New("Invalid last id")
	}
	return &OperationLastID{oid}, nil
}

func (rid ReplicationLastID) String() string {
	return strconv.FormatInt(rid.int64, 10)
}

// Time extract the time from the replication id
func (rid ReplicationLastID) Time() time.Time {
	return time.Unix(0, rid.int64*1000000)
}

func (oid OperationLastID) String() string {
	return oid.ObjectId.Hex()
}

// Fallback tries to convert a "event" id into a "replication" id by extracting
// the timestamp part of the Mongo ObjectId. If the id is not a valid ObjectId,
// an error is returned.
func (oid *OperationLastID) Fallback() LastID {
	return &ReplicationLastID{oid.Time().UnixNano() / 1000000, true}
}
