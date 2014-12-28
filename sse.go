package oplog

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	log "github.com/Sirupsen/logrus"
)

// SSEDaemon listens for events and send them to the oplog MongoDB capped collection
type SSEDaemon struct {
	s        *http.Server
	ol       *OpLog
	Password string
}

func NewSSEDaemon(addr string, ol *OpLog) *SSEDaemon {
	daemon := &SSEDaemon{nil, ol, ""}
	daemon.s = &http.Server{
		Addr:           addr,
		Handler:        daemon,
		MaxHeaderBytes: 1 << 20,
	}

	return daemon
}

// authenticate checks for HTTP basic authentication if an admin password is set.
func (daemon *SSEDaemon) authenticate(r *http.Request) bool {
	if daemon.Password == "" {
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

	return daemon.Password == pair[1]
}

func (daemon *SSEDaemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(405)
		return
	}
	switch r.URL.Path {
	case "/status":
		daemon.Status(w, r)
	case "/ops", "/":
		daemon.Ops(w, r)
	default:
		w.WriteHeader(404)
	}
}

func (daemon *SSEDaemon) Status(w http.ResponseWriter, r *http.Request) {
	status, err := json.Marshal(daemon.ol.Status)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(status)
}

func (daemon *SSEDaemon) Ops(w http.ResponseWriter, r *http.Request) {
	log.Info("SSE connection started")

	if r.Header.Get("Accept") != "text/event-stream" {
		// Not an event stream request, return a 406 Not Acceptable HTTP error
		w.WriteHeader(406)
		return
	}

	if !daemon.authenticate(r) {
		w.WriteHeader(401)
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

	types := []string{}
	if r.URL.Query().Get("types") != "" {
		types = strings.Split(r.URL.Query().Get("types"), ",")
	}
	parents := []string{}
	if r.URL.Query().Get("parents") != "" {
		parents = strings.Split(r.URL.Query().Get("parents"), ",")
	}
	filter := OpLogFilter{
		Types:   types,
		Parents: parents,
	}

	flusher := w.(http.Flusher)
	notifier := w.(http.CloseNotifier)
	ops := make(chan io.WriterTo)
	err := make(chan error)
	flusher.Flush()

	go daemon.ol.Tail(lastId, filter, ops, err)
	daemon.ol.Status.Clients.Incr()

	for {
		select {
		case <-notifier.CloseNotify():
			log.Info("SSE connection closed")
			daemon.ol.Status.Clients.Decr()
			return
		case err := <-err:
			log.Warnf("SSE oplog error %s", err)
			return
		case op := <-ops:
			log.Debug("SSE sending event")
			_, err := op.WriteTo(w)
			if err != nil {
				log.Warnf("SSE write error %s", err)
				continue
			}
			flusher.Flush()
		}
	}
}

func (daemon *SSEDaemon) Run() error {
	return daemon.s.ListenAndServe()
}
