package oplog

import (
	"encoding/json"
	"strings"
	"time"
)

// inOperation represents an Operation ingested as JSON.
type inOperation struct {
	Event     string     `json:"event"`
	Parents   []string   `json:"parents"`
	Type      string     `json:"type"`
	ID        string     `json:"id"`
	Timestamp *time.Time `json:"timestamp,omniempty"`
}

// decodeOperation parses JSON data and returns an Operation on success.
func decodeOperation(data []byte) (*Operation, error) {
	operation := inOperation{}
	err := json.Unmarshal(data, &operation)
	if err != nil {
		return nil, err
	}

	// The timestamp field is optional
	var timestamp time.Time
	if operation.Timestamp != nil {
		timestamp = *operation.Timestamp
	} else {
		timestamp = time.Now()
	}

	op := &Operation{
		Event: strings.ToLower(operation.Event),
		Data: &OperationData{
			Timestamp: timestamp,
			Parents:   operation.Parents,
			Type:      strings.ToLower(operation.Type),
			ID:        operation.ID,
		},
	}
	if err := op.Validate(); err != nil {
		return nil, err
	}
	return op, nil
}
