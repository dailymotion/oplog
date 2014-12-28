package oplog

import (
	"strconv"
	"sync/atomic"
)

type counter struct {
	uint64
}

type gauge struct {
	int64
}

// Stats stores all the statistics about the oplog
type Stats struct {
	Status string `json:"status"`
	// Total number of events recieved on the UDP interface
	EventsReceived *counter `json:"events_received"`
	// Total number of events ingested into MongoDB with success
	EventsIngested *counter `json:"events_ingested"`
	// Total number of events received on the UDP interface with an invalid format
	EventsError *counter `json:"events_error"`
	// Total number of events discarded because the queue was full
	EventsDiscarded *counter `json:"events_discarded"`
	// Current number of events in the ingestion queue
	QueueSize *gauge `json:"queue_size"`
	// Maximum number of events allowed in the ingestion queue before discarding events
	QueueMaxSize *gauge `json:"queue_max_size"`
	// Number of clients connected to the SSE API
	Clients *gauge `json:"clients"`
}

// NewStats create a new empty stats object
func NewStats() Stats {
	return Stats{
		Status:          "OK",
		EventsReceived:  &counter{0},
		EventsIngested:  &counter{0},
		EventsError:     &counter{0},
		EventsDiscarded: &counter{0},
		QueueSize:       &gauge{0},
		QueueMaxSize:    &gauge{0},
		Clients:         &gauge{0},
	}
}

func (c *counter) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatUint(c.uint64, 10)), nil
}

// Incr atomically increment the counter by 1
func (c *counter) Incr() {
	atomic.AddUint64(&c.uint64, 1)
}

func (g *gauge) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(g.int64, 10)), nil
}

// Set atomically set the gauge value to the provided int
func (g *gauge) Set(val int) {
	atomic.StoreInt64(&g.int64, int64(val))
}

// Incr atomically increment the gauge by 1
func (g *gauge) Incr() {
	atomic.AddInt64(&g.int64, 1)
}

// Incr atomically decrement the gauge by 1
func (g *gauge) Decr() {
	atomic.AddInt64(&g.int64, -1)
}
