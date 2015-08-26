package oplog

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// objectState is the current state of an object given the most recent operation applied on it
type objectState struct {
	ID        string         `bson:"_id,omitempty" json:"id"`
	Event     string         `bson:"event"`
	Timestamp time.Time      `bson:"ts"`
	Data      *OperationData `bson:"data"`
}

// GetEventID returns an SSE last event id for the object state
func (obj objectState) GetEventID() LastID {
	return &ReplicationLastID{obj.Timestamp.UnixNano() / 1000000, false}
}

// WriteTo serializes an objectState as a SSE compatible message
func (obj objectState) WriteTo(w io.Writer) (int64, error) {
	data, err := json.Marshal(obj.Data)
	if err != nil {
		return 0, err
	}
	n, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", obj.Timestamp.UnixNano()/1000000, obj.Event, data)
	return int64(n), err
}
