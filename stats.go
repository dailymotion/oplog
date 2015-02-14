package oplog

import "expvar"

// Stats stores all the statistics about the oplog
type Stats struct {
	Status string
	// Total number of events recieved on the UDP interface
	EventsReceived *expvar.Int
	// Total number of events sent thru the SSE interface
	EventsSent *expvar.Int
	// Total number of events ingested into MongoDB with success
	EventsIngested *expvar.Int
	// Total number of events received on the UDP interface with an invalid format
	EventsError *expvar.Int
	// Total number of events discarded because the queue was full
	EventsDiscarded *expvar.Int
	// Current number of events in the ingestion queue
	QueueSize *expvar.Int
	// Maximum number of events allowed in the ingestion queue before discarding events
	QueueMaxSize *expvar.Int
	// Number of clients connected to the SSE API
	Clients *expvar.Int
	// Total number of SSE connections
	Connections *expvar.Int
}

// NewStats create a new empty stats object
func NewStats() Stats {
	return Stats{
		Status:          "OK",
		EventsReceived:  expvar.NewInt("events_received"),
		EventsSent:      expvar.NewInt("events_sent"),
		EventsIngested:  expvar.NewInt("events_ingested"),
		EventsError:     expvar.NewInt("events_error"),
		EventsDiscarded: expvar.NewInt("events_discarded"),
		QueueSize:       expvar.NewInt("queue_size"),
		QueueMaxSize:    expvar.NewInt("queue_max_size"),
		Clients:         expvar.NewInt("clients"),
		Connections:     expvar.NewInt("connections"),
	}
}
