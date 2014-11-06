package oplog

import (
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

// SSEDaemon listens for events and send them to the oplog MongoDB capped collection
type SSEDaemon struct {
	s  *http.Server
	ol *OpLog
}

func NewSSEDaemon(port int, ol *OpLog) *SSEDaemon {
	daemon := &SSEDaemon{nil, ol}
	daemon.s = &http.Server{
		Addr:           fmt.Sprintf(":%d", port),
		Handler:        daemon,
		MaxHeaderBytes: 1 << 20,
	}

	return daemon
}

func (daemon *SSEDaemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Info("SSE connection started")

	if r.Header.Get("Accept") != "text/event-stream" {
		// Not an event stream request, return a 406 Not Acceptable HTTP error
		w.WriteHeader(406)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Connection", "keep-alive")
	h.Set("Access-Control-Allow-Origin", "*")

	lastId := r.Header.Get("Last-Event-ID")
	if daemon.ol.HasId(lastId) {
		h.Set("Last-Event-ID", lastId)
	} else {
		// If requested id doesn't exists, start the stream from the very last
		// operation in the oplog and do not return the Last-Event-ID header
		// to indicate we didn't obey the request
		lastId = daemon.ol.LastId()
	}

	flusher := w.(http.Flusher)
	notifier := w.(http.CloseNotifier)
	ops := make(chan Operation)
	err := make(chan error)
	flusher.Flush()

	go daemon.ol.Tail(lastId, ops, err)

	for {
		select {
		case <-notifier.CloseNotify():
			log.Info("SSE connection closed")
			return
		case err := <-err:
			log.Warnf("SSE oplog error %s", err)
			return
		case op := <-ops:
			log.Debugf("SSE sending event %s", op.Id.Hex())
			data, err := json.Marshal(op.Data)
			if err != nil {
				log.Warnf("SSE JSON encoding error %s", err)
				continue
			}
			fmt.Fprintf(w, "id: %s\n", op.Id.Hex())
			fmt.Fprintf(w, "event: %s\n", op.Event)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (daemon *SSEDaemon) Run() error {
	return daemon.s.ListenAndServe()
}
