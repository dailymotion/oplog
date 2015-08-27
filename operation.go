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
	ID    *bson.ObjectId `bson:"_id,omitempty"`
	Event string         `bson:"event"`
	Data  *OperationData `bson:"data"`
}

// OperationData is the data part of the SSE event for the operation.
type OperationData struct {
	Timestamp time.Time `bson:"ts" json:"timestamp"`
	Parents   []string  `bson:"p" json:"parents"`
	Type      string    `bson:"t" json:"type"`
	ID        string    `bson:"id" json:"id"`
	Ref       string    `bson:"-,omitempty" json:"ref,omitempty"`
}

// NewOperation creates an new operation from given information.
//
// The event argument can be one of "insert", "update" or "delete". The time
// defines the exact modification date of the object (must be the exact same time
// as stored in the database).
func NewOperation(event string, time time.Time, objID, objType string, objParents []string) *Operation {
	id := bson.NewObjectId()
	return &Operation{
		ID:    &id,
		Event: event,
		Data: &OperationData{
			Timestamp: time,
			ID:        objID,
			Type:      objType,
			Parents:   objParents,
		},
	}
}

// GetEventID returns an SSE last event id for the operation
func (op Operation) GetEventID() LastID {
	return &OperationLastID{op.ID}
}

// Validate ensures an operation has the proper syntax
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
	n, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", op.ID.Hex(), op.Event, data)
	return int64(n), err
}

// Info returns a human readable version of the operation
func (op *Operation) Info() string {
	id := "(new)"
	if op.ID != nil {
		id = op.ID.Hex()
	}
	return fmt.Sprintf("%s:%s(%s:%s)", id, op.Event, op.Data.Type, op.Data.ID)
}

// genRef generates the reference URL (Ref field) from the given object URL template based on
// the Type and Id fields.
func (obd *OperationData) genRef(objectURL string) {
	if objectURL == "" {
		obd.Ref = ""
		return
	}

	r := strings.NewReplacer("{{type}}", obd.Type, "{{id}}", obd.ID)
	obd.Ref = r.Replace(objectURL)
}

// GetID returns the operation id
func (obd OperationData) GetID() string {
	b := bytes.Buffer{}
	b.WriteString(obd.Type)
	b.WriteString("/")
	b.WriteString(obd.ID)
	return b.String()
}

// Validate ensures an operation data has the right syntax
func (obd OperationData) Validate() error {
	if obd.ID == "" {
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
