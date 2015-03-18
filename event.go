package oplog

import (
	"fmt"
	"io"
	"time"
)

// GenericEvent is an interface used by the oplog to send different kinds of
// SSE compatible events
type GenericEvent interface {
	io.WriterTo
	GetEventId() LastId
}

type GenericLastEventId struct {
	string
}

// OpLogEvent is used to send "technical" events with no data like "reset" or "live"
type OpLogEvent struct {
	Id    string
	Event string
}

// GetEventId returns an SSE event id
func (e OpLogEvent) GetEventId() LastId {
	return &GenericLastEventId{e.Id}
}

// WriteTo serializes an event as a SSE compatible message
func (e OpLogEvent) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "id: %s\nevent: %s\n\n", e.GetEventId(), e.Event)
	return int64(n), err
}

func (gid *GenericLastEventId) String() string {
	return gid.string
}

func (gid *GenericLastEventId) Time() time.Time {
	return time.Time{}
}
