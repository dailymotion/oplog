package oplog

import (
	"encoding/base64"
	"expvar"
	"fmt"
	"io"
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
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"status\":\"OK\"")
	expvar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(w, ",%q:%s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "}")
}

func (daemon *SSEDaemon) Ops(w http.ResponseWriter, r *http.Request) {
	ip := xff.GetRemoteAddr(r)
	log.Infof("SSE[%s] connection started", ip)

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
	h.Set("Server", fmt.Sprintf("oplog/%s", VERSION))
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Connection", "close")
	h.Set("Access-Control-Allow-Origin", "*")

	var lastId LastId
	var err error
	if r.Header.Get("Last-Event-ID") == "" {
		// No last id provided, use the very last id of the events collection
		lastId, err = daemon.ol.LastId()
		if err != nil {
			log.Warnf("SSE[%s] can't get last id: %s", ip, err)
			w.WriteHeader(503)
			return
		}
	} else {
		if lastId, err = NewLastId(r.Header.Get("Last-Event-ID")); err != nil {
			log.Warnf("SSE[%s] invalid last id: %s", ip, err)
			w.WriteHeader(400)
			return
		}
		found, err := daemon.ol.HasId(lastId)
		if err != nil {
			log.Warnf("SSE[%s] can't check last id: %s", ip, err)
			w.WriteHeader(503)
			return
		}
		if !found {
			log.Debugf("SSE[%s] last id not found, falling back to replication id: %s", ip, lastId.String())
			// If the requested event id is not found, fallback to a replication id
			olid := lastId.(*OperationLastId)
			lastId = olid.Fallback()
		}
		// Backward compat, remove when all oplogc will be updated
		h.Set("Last-Event-ID", r.Header.Get("Last-Event-ID"))
	}

	if lastId != nil {
		log.Debugf("SSE[%s] using last id: %s", ip, lastId.String())
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
	stop := make(chan bool)
	flusher.Flush()

	go daemon.ol.Tail(lastId, filter, ops, stop)
	defer func() {
		// Stop the oplog tailer
		stop <- true
	}()

	daemon.ol.Stats.Clients.Add(1)
	daemon.ol.Stats.Connections.Add(1)
	defer daemon.ol.Stats.Clients.Add(-1)

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
			flusher.Flush()
		case <-time.After(25 * time.Second):
			// Send "ping" data to prevent proxy/browsers from closing the connection
			// for inactivity
			log.Debugf("SSE[%s] sending a keep alive ping", ip)
			if _, err := w.Write([]byte{':', '\n'}); err != nil {
				log.Warnf("SSE[%s] write error: ", ip, err)
				return
			}
			flusher.Flush()
		}
	}
}

func (daemon *SSEDaemon) Run() error {
	return daemon.s.ListenAndServe()
}
