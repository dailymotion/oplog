package oplog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"gopkg.in/mgo.v2/bson"
)

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

// GetEventId returns an SSE event id as string for the operation
func (op Operation) GetEventId() string {
	return op.Id.Hex()
}

func (op Operation) Validate() error {
	switch op.Event {
	case "create", "update", "delete":
	default:
		return fmt.Errorf("invalid event name: %s", op.Event)
	}
	return op.Data.Validate()
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

func (obd OperationData) GetId() string {
	b := bytes.Buffer{}
	b.WriteString(obd.Type)
	b.WriteString("/")
	b.WriteString(obd.Id)
	return b.String()
}

func (obd OperationData) Validate() error {
	if obd.Id == "" {
		return errors.New("missing id field")
	}
	if obd.Type == "" {
		return errors.New("missing type field")
	}
	for _, parent := range obd.Parents {
		if parent == "" {
			return errors.New("parent can't be empty")
		}
	}
	return nil
}
