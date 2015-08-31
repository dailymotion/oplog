package oplog

import (
	"encoding/base64"
	"expvar"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/sebest/xff"
)

// SSEDaemon listens for events and send them to the oplog MongoDB capped collection
type SSEDaemon struct {
	s  *http.Server
	ol *OpLog
	// Password is the shared secret to connect to a password protected oplog.
	Password string
	// IngestPassword is the shared secret to connect to the HTTP ingest endpoint.
	IngestPassword string
	// FlushInterval defines the interval between flushes of the HTTP socket.
	FlushInterval time.Duration
	// HeartbeatTickerCount defines the number of FlushInterval with nothing to flush
	// is required before we send an heartbeat.
	HeartbeatTickerCount int8
}

// NewSSEDaemon creates a new HTTP server configured to serve oplog stream over HTTP
// using Server Sent Event protocol.
func NewSSEDaemon(addr string, ol *OpLog) *SSEDaemon {
	daemon := &SSEDaemon{
		ol:                   ol,
		Password:             "",
		FlushInterval:        500 * time.Millisecond,
		HeartbeatTickerCount: 50, // 25 seconds
	}
	daemon.s = &http.Server{
		Addr:           addr,
		Handler:        daemon,
		MaxHeaderBytes: 1 << 20,
	}

	return daemon
}

// checkPassword checks HTTP basic authentication's password.
func checkPassword(r *http.Request, password string) bool {
	if password == "" {
		return true
	}

	s := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 || s[0] != "Basic" {
		return false
	}

	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return false
	}
	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		return false
	}

	return password == pair[1]
}

func (daemon *SSEDaemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/status":
		if r.Method == "GET" {
			daemon.Status(w, r)
		} else {
			w.WriteHeader(405)
			return
		}
	case "/ops", "/":
		if r.Method == "GET" {
			daemon.GetOps(w, r)
		} else if r.Method == "POST" {
			daemon.PostOps(w, r)
		} else {
			w.WriteHeader(405)
			return
		}
	default:
		w.WriteHeader(404)
	}
}

// Status exposes expvar data
func (daemon *SSEDaemon) Status(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"status\":\"OK\"")
	expvar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(w, ",%q:%s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "}")
}

// PostOps exposes an endpoint to POST operations
func (daemon *SSEDaemon) PostOps(w http.ResponseWriter, r *http.Request) {
	if !checkPassword(r, daemon.IngestPassword) {
		w.WriteHeader(401)
		return
	}

	if r.Header.Get("Content-Type") != "application/json" {
		w.WriteHeader(415)
		return
	}

	h := w.Header()
	h.Set("Server", fmt.Sprintf("oplog/%s", Version))
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Connection", "close")
	h.Set("Access-Control-Allow-Origin", "*")

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Warnf("HTTP ingest error reading Body: %s", err)
		daemon.ol.Stats.EventsError.Add(1)
		w.WriteHeader(503)
		return
	}

	op, err := ingestOperation(body)
	if err != nil {
		log.Warnf("HTTP ingest invalid operation received: %s", err)
		daemon.ol.Stats.EventsError.Add(1)
		w.WriteHeader(503)
		return
	}

	daemon.ol.Append(op)
	daemon.ol.Stats.EventsReceived.Add(1)
	w.WriteHeader(204)
}

// GetOps exposes an SSE endpoint to stream operations
func (daemon *SSEDaemon) GetOps(w http.ResponseWriter, r *http.Request) {
	ip := xff.GetRemoteAddr(r)
	log.Infof("SSE[%s] connection started", ip)

	if r.Header.Get("Accept") != "text/event-stream" {
		// Not an event stream request, return a 406 Not Acceptable HTTP error
		w.WriteHeader(406)
		return
	}

	if !checkPassword(r, daemon.Password) {
		w.WriteHeader(401)
		return
	}

	h := w.Header()
	h.Set("Server", fmt.Sprintf("oplog/%s", Version))
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Connection", "close")
	h.Set("Access-Control-Allow-Origin", "*")

	var lastID LastID
	var err error
	if r.Header.Get("Last-Event-ID") == "" {
		// No last id provided, use the very last id of the events collection
		lastID, err = daemon.ol.LastID()
		if err != nil {
			log.Warnf("SSE[%s] can't get last id: %s", ip, err)
			w.WriteHeader(503)
			return
		}
	} else {
		if lastID, err = NewLastID(r.Header.Get("Last-Event-ID")); err != nil {
			log.Warnf("SSE[%s] invalid last id: %s", ip, err)
			w.WriteHeader(400)
			return
		}
		found, err := daemon.ol.HasID(lastID)
		if err != nil {
			log.Warnf("SSE[%s] can't check last id: %s", ip, err)
			w.WriteHeader(503)
			return
		}
		if !found {
			log.Debugf("SSE[%s] last id not found, falling back to replication id: %s", ip, lastID.String())
			// If the requested event id is not found, fallback to a replication id
			olid := lastID.(*OperationLastID)
			lastID = olid.Fallback()
		}
		// Backward compat, remove when all oplogc will be updated
		h.Set("Last-Event-ID", r.Header.Get("Last-Event-ID"))
	}

	if lastID != nil {
		log.Debugf("SSE[%s] using last id: %s", ip, lastID.String())
	}

	types := []string{}
	if r.URL.Query().Get("types") != "" {
		types = strings.Split(r.URL.Query().Get("types"), ",")
	}
	parents := []string{}
	if r.URL.Query().Get("parents") != "" {
		parents = strings.Split(r.URL.Query().Get("parents"), ",")
	}
	filter := Filter{
		Types:   types,
		Parents: parents,
	}

	flusher := w.(http.Flusher)
	notifier := w.(http.CloseNotifier)
	ops := make(chan GenericEvent)
	stop := make(chan bool)
	flusher.Flush()

	go daemon.ol.Tail(lastID, filter, ops, stop)
	defer func() {
		// Stop the oplog tailer
		stop <- true
	}()

	daemon.ol.Stats.Clients.Add(1)
	daemon.ol.Stats.Connections.Add(1)
	defer daemon.ol.Stats.Clients.Add(-1)

	// Messages are buffered and flushed every daemon.FlushInterval to save I/Os
	ticker := time.NewTicker(daemon.FlushInterval)
	defer ticker.Stop()
	var empty int8

	for {
		select {
		case <-notifier.CloseNotify():
			log.Infof("SSE[%s] connection closed", ip)
			return

		case op := <-ops:
			log.Debugf("SSE[%s] sending event", ip)
			daemon.ol.Stats.EventsSent.Add(1)
			if _, err := op.WriteTo(w); err != nil {
				log.Warnf("SSE[%s] write error: %s", ip, err)
				return
			}
			empty = -1

		case <-ticker.C:
			// Flush the buffer at regular interval
			if empty >= 0 {
				// Skip if buffer has no data, if empty for too long, send a heartbeat
				if empty >= daemon.HeartbeatTickerCount {
					if _, err := w.Write([]byte{':', '\n'}); err != nil {
						log.Warnf("SSE[%s] write error: %s", ip, err)
						return
					}
				} else {
					empty++
					continue
				}
			}
			empty = 0
			log.Debugf("SSE[%s] flushing buffer", ip)
			flusher.Flush()
		}
	}
}

// Run starts the SSE server
func (daemon *SSEDaemon) Run() error {
	return daemon.s.ListenAndServe()
}
