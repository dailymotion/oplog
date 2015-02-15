package oplog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
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
	Ref       string    `bson:"-,omitempty" json:"ref,omitempty"`
}

// ObjectState is the current state of an object given the most recent operation applied on it
type ObjectState struct {
	Id        string         `bson:"_id,omitempty" json:"id"`
	Event     string         `bson:"event"`
	Timestamp time.Time      `bson:"ts"`
	Data      *OperationData `bson:"data"`
}

// GetEventId returns an SSE last event id for the operation
func (op Operation) GetEventId() LastId {
	return &OperationLastId{op.Id}
}

func (op Operation) Validate() error {
	switch op.Event {
	case "insert", "update", "delete":
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
	n, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", op.Id.Hex(), op.Event, data)
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

// GetEventId returns an SSE last event id for the object state
func (obj ObjectState) GetEventId() LastId {
	return &ReplicationLastId{obj.Timestamp.UnixNano() / 1000000, false}
}

// WriteTo serializes an ObjectState as a SSE compatible message
func (obj ObjectState) WriteTo(w io.Writer) (int64, error) {
	data, err := json.Marshal(obj.Data)
	if err != nil {
		return 0, err
	}
	n, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", obj.Timestamp.UnixNano()/1000000, obj.Event, data)
	return int64(n), err
}

// genRef generates the reference URL (Ref field) from the given object URL template based on
// the Type and Id fields.
func (obd *OperationData) genRef(objectURL string) {
	if objectURL == "" {
		obd.Ref = ""
		return
	}

	r := strings.NewReplacer("{{type}}", obd.Type, "{{id}}", obd.Id)
	obd.Ref = r.Replace(objectURL)
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
