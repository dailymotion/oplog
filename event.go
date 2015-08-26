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
	GetEventID() LastID
}

// genericLastID stores an arbitrary event id
type genericLastID string

// Event is used to send "technical" events with no data like "reset" or "live"
type Event struct {
	ID    string
	Event string
}

// GetEventID returns an SSE event id
func (e Event) GetEventID() LastID {
	i := genericLastID(e.ID)
	return &i
}

// WriteTo serializes an event as a SSE compatible message
func (e Event) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "id: %s\nevent: %s\n\n", e.GetEventID(), e.Event)
	return int64(n), err
}

func (gid genericLastID) String() string {
	return string(gid)
}

// Time returns a zero time a GenericLastID doest not hold any time information
func (gid genericLastID) Time() time.Time {
	return time.Time{}
}
