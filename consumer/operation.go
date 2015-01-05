package consumer

import "time"

type Operation struct {
	ID    string
	Event string
	Data  *OperationData
	ack   chan<- Operation
}

// OperationData is the data part of the SSE event for the operation.
type OperationData struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Ref       string    `json:"ref,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Parents   []string  `json:"parents"`
}

// Done must be called once the operation has been processed by the consumer
func (o *Operation) Done() {
	o.ack <- *o
}
