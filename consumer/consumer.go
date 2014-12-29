package consumer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Options is the subscription options
type Options struct {
	// Path of the state file where to persiste the current oplog position.
	// If empty string, the state is not stored.
	StateFile string
	// Password to access password protected oplog
	Password string
	// Filters to apply on the oplog output
	Filter Filter
}

// Filter contains arguments to filter the oplog output
type Filter struct {
	// A list of types to filter on
	Types []string
	// A list of parent type/id to filter on
	Parents []string
}

// Consumer holds all the information required to connect to an oplog server
type Consumer struct {
	// URL of the oplog
	url string
	// options for the consumer's subscription
	options Options
	// lastId is the current most advanced acked event id
	lastId string
	// saved is true when current lastId is persisted
	saved bool
	// mtx is a mutex used to coordinate access to lastId and saved properties
	mtx *sync.RWMutex
	// http is the client used to connect to the oplog
	http http.Client
	// body points to the current streamed response body
	body io.ReadCloser
	// ife holds all event ids sent to the consumer but no yet acked
	ife *InFlightEvents
}

// ErrorAccessDenied is returned by Subscribe when the oplog requires a password
// different from the one provided in options.
var ErrorAccessDenied = errors.New("Invalid credentials")

// Subscribe creates a Consumer to connect to the given URL.
//
// If the oplog is password protected and invalid credentials has been set,
// the ErrorAccessDenied will be returned. Any other connection errors won't
// generate errors, the Process() method will try to reconnect until the server
// is reachable again.
func Subscribe(url string, options Options) (*Consumer, error) {
	qs := ""
	if len(options.Filter.Parents) > 0 {
		parents := strings.Join(options.Filter.Parents, ",")
		if parents != "" {
			qs += "?parents="
			qs += parents
		}
	}
	if len(options.Filter.Types) > 0 {
		types := strings.Join(options.Filter.Types, ",")
		if types != "" {
			if qs == "" {
				qs += "?"
			} else {
				qs += "&"
			}
			qs += "types="
			qs += types
		}
	}

	c := &Consumer{
		url:     strings.Join([]string{url, qs}, ""),
		options: options,
		ife:     NewInFlightEvents(),
		mtx:     &sync.RWMutex{},
	}

	// Recover the last event id saved from a previous excution
	lastId, err := c.loadLastEventID()
	if err != nil {
		return nil, err
	}
	c.lastId = lastId

	// Try to connect, if a 403 or 401 is returned, return an error
	// otherwise ignore any other error as Process() will retry in loop
	// until the oplog becomes available.
	if err := c.connect(); err == ErrorAccessDenied {
		return nil, err
	}

	return c, nil
}

// Process reads the oplog output and send operations back thru the given ops channel.
// The caller must then send operations back thru the ack channel once the operation has
// been handled. Failing to ack the operations would prevent any resume in case of
// connection failure or restart of the process.
//
// Note that some non recoverable errors may throw a fatal error.
func (c *Consumer) Process(ops chan<- Operation, ack <-chan Operation) {
	go func() {
		d := NewDecoder(c.body)
		op := Operation{}
		for {
			err := d.Next(&op)
			if err != nil {
				log.Printf("OPLOG error: %s", err)
				backoff := time.Second
				for {
					time.Sleep(backoff)
					if err = c.connect(); err == nil {
						d = NewDecoder(c.body)
						break
					}
					log.Printf("OPLOG conn error: %s", err)
					if backoff < 30*time.Second {
						backoff *= 2
					}
				}
				continue
			}

			c.ife.Push(op.ID)
			if op.Event == "reset" {
				// We must not process any further operation until the "reset" operation
				// is not acked
				c.ife.Lock()
			}
			ops <- op
		}
	}()

	// Periodic (non blocking) saving of the last id when needed
	if c.options.StateFile != "" {
		go func() {
			for {
				time.Sleep(time.Second)
				c.mtx.RLock()
				saved := c.saved
				lastId := c.lastId
				c.mtx.RUnlock()
				if saved {
					continue
				}
				if err := c.saveLastEventID(lastId); err != nil {
					log.Fatalf("OPLOG can't persist last event ID processed: %v", err)
				}
				c.mtx.Lock()
				c.saved = lastId == c.lastId
				c.mtx.Unlock()
			}
		}()
	}

	for {
		op := <-ack
		if op.Event == "reset" {
			c.ife.Unlock()
		}
		if found, first := c.ife.Pull(op.ID); found && first {
			c.setLastId(op.ID)
		}
	}
}

// LastId returns the most advanced acked event id
func (c *Consumer) LastId() string {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.lastId
}

// setLastId sets the last id to the given value and informs the save go routine
func (c *Consumer) setLastId(id string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.lastId = id
	c.saved = false
}

// connect tries to connect to the oplog event stream
func (c *Consumer) connect() (err error) {
	if c.body != nil {
		c.body.Close()
	}
	// Usable dummy body in case of connection error
	c.body = ioutil.NopCloser(bytes.NewBuffer([]byte{}))

	req, err := http.NewRequest("GET", c.url, nil)
	if err != nil {
		return
	}
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Accept", "text/event-stream")
	lastId := c.LastId()
	if len(lastId) > 0 {
		req.Header.Set("Last-Event-ID", lastId)
	}
	if c.options.Password != "" {
		req.SetBasicAuth("", c.options.Password)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return
	}
	if lastId != "" && res.Header.Get("Last-Event-ID") != lastId {
		// If the response doesn't contain the requested Last-Event-ID
		// header, it means the resume did fail. This is not a recoverable
		// error, the operator must either decide to perform a full replication
		// or accept to loose events by truncating the state file.
		log.Fatal("OPLOG resume failed")
		return
	}
	if res.StatusCode == 403 || res.StatusCode == 401 {
		err = ErrorAccessDenied
		return
	}
	if res.StatusCode != 200 {
		message, _ := ioutil.ReadAll(res.Body)
		err = fmt.Errorf("HTTP error %d: %s", res.StatusCode, string(message))
		return
	}
	c.body = res.Body
	return
}

// loadLastEventID tries to read the last event id from the state file.
//
// If the StateFile option was not set, the id will always be an empty string
// as for tailing only future events.
//
// If the StateFile option is set but no file exists, the last event id is
// initialized to "0" in order to request a full replication.
func (c *Consumer) loadLastEventID() (id string, err error) {
	if c.options.StateFile == "" {
		return "", nil
	}
	_, err = os.Stat(c.options.StateFile)
	if os.IsNotExist(err) {
		// full replication
		id = "0"
		err = nil
	} else if err == nil {
		var content []byte
		content, err = ioutil.ReadFile(c.options.StateFile)
		if err != nil {
			return
		}
		if match, _ := regexp.Match("^(?:[0-9]{0,13}|[0-9a-f]{24})$", content); !match {
			err = errors.New("state file contains invalid data")
		}
		id = string(content)
	}
	return
}

// saveLastEventID persiste the last event id into a file
func (c *Consumer) saveLastEventID(id string) error {
	return ioutil.WriteFile(c.options.StateFile, []byte(id), 0644)
}
