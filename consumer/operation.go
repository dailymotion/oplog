package consumer

import "time"

type Operation struct {
	ID    string
	Event string
	Data  *OperationData
}

// OperationData is the data part of the SSE event for the operation.
type OperationData struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Parents   []string  `json:"parents"`
}
