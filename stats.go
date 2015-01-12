package oplog

import (
	"bytes"
	"fmt"
	"strconv"
	"sync/atomic"
)

type field interface {
	name() string
	value() string
}

type counter struct {
	uint64
	n string
}

type gauge struct {
	int64
	n string
}

// Stats stores all the statistics about the oplog
type Stats struct {
	Status string
	// Total number of events recieved on the UDP interface
	EventsReceived *counter
	// Total number of events ingested into MongoDB with success
	EventsIngested *counter
	// Total number of events received on the UDP interface with an invalid format
	EventsError *counter
	// Total number of events discarded because the queue was full
	EventsDiscarded *counter
	// Current number of events in the ingestion queue
	QueueSize *gauge
	// Maximum number of events allowed in the ingestion queue before discarding events
	QueueMaxSize *gauge
	// Number of clients connected to the SSE API
	Clients *gauge
}

// NewStats create a new empty stats object
func NewStats() Stats {
	return Stats{
		Status:          "OK",
		EventsReceived:  &counter{0, "events_received"},
		EventsIngested:  &counter{0, "events_ingested"},
		EventsError:     &counter{0, "events_error"},
		EventsDiscarded: &counter{0, "events_discarded"},
		QueueSize:       &gauge{0, "queue_size"},
		QueueMaxSize:    &gauge{0, "queue_max_size"},
		Clients:         &gauge{0, "clients"},
	}
}

func (s Stats) MarshalJSON() ([]byte, error) {
	b := &bytes.Buffer{}
	b.WriteByte('{')
	writeRawField("status", fmt.Sprintf("\"%s\"", s.Status), b, false)
	writeField(s.EventsReceived, b, false)
	writeField(s.EventsIngested, b, false)
	writeField(s.EventsError, b, false)
	writeField(s.EventsDiscarded, b, false)
	writeField(s.QueueSize, b, false)
	writeField(s.QueueMaxSize, b, false)
	writeField(s.Clients, b, true)
	b.WriteByte('}')
	return b.Bytes(), nil
}

func writeField(field field, b *bytes.Buffer, last bool) {
	writeRawField(field.name(), field.value(), b, last)
}

func writeRawField(name string, value string, b *bytes.Buffer, last bool) {
	b.WriteByte('"')
	b.WriteString(name)
	b.WriteString("\":")
	b.WriteString(value)
	if !last {
		b.WriteByte(',')
	}
}

func (c *counter) name() string {
	return c.n
}

func (c *counter) value() string {
	return strconv.FormatUint(atomic.LoadUint64(&c.uint64), 10)
}

// Incr atomically increment the counter by 1
func (c *counter) Incr() {
	atomic.AddUint64(&c.uint64, 1)
}

func (g *gauge) name() string {
	return g.n
}

func (g *gauge) value() string {
	return strconv.FormatInt(atomic.LoadInt64(&g.int64), 10)
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
