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
	"path"
	"strings"
	"time"
)

// Options is the subscription options
type Options struct {
	// Path of the state file where to persiste the current oplog position
	StateFile string
	// Password to access password protected oplog
	Password string
	// Filters to apply on the oplog output
	Filter Filter
	// Reset removes any saved state and forces a full replication
	Reset bool
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
	url     string
	options Options
	lastId  string
	http    http.Client
	body    io.ReadCloser
	ife     *InFlightEvents
}

var ErrorAccessDenied = errors.New("Invalid credentials")

// Subscribe creates a Consumer to connect to the given URL.
//
// If the oplog is password protected and invalid credentials has been set,
// the ErrorAccessDenied will be returned. Any other connection errors won't
// generate errors, the Process method will try to reconnect until the server
// is reachable again.
func Subscribe(url string, options Options) (*Consumer, error) {
	if options.StateFile == "" {
		options.StateFile = path.Join(os.TempDir(), "oplog.state")
	}

	if options.Reset {
		os.Remove(options.StateFile)
	}

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
	}

	lastId, err := c.loadLastEventID()
	if err != nil {
		return nil, err
	}
	c.lastId = lastId

	if err := c.connect(); err == ErrorAccessDenied {
		return nil, err
	}

	return c, nil
}

// Process reads the oplog output and send operations back thru the given ops channel.
// The caller must then send operations back thru the ack channel once the operation has
// been handled. Failing to ack the operations would prevent any resume in case of
// connection failure or restart of the process.
func (c *Consumer) Process(ops chan<- *Operation, ack <-chan *Operation) {
	if c.lastId == "0" {
		// On full replication, send a "reset" operation
		ops <- &Operation{
			ID:    "0",
			Event: "reset",
		}
	}

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
					if backoff < 60*time.Second {
						backoff *= 2
					}
				}
				continue
			}

			c.ife.Push(op.ID)
			ops <- &op
		}
	}()

	for {
		op := <-ack
		if found, first := c.ife.Pull(op.ID); found && first {
			if err := c.saveLastEventID(op.ID); err != nil {
				log.Fatalf("Can't persist last event ID processed: %v", err)
			}
			c.lastId = op.ID
		}
	}
}

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
	if len(c.lastId) > 0 {
		req.Header.Set("Last-Event-ID", c.lastId)
	}
	if c.options.Password != "" {
		req.SetBasicAuth("", c.options.Password)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return
	}
	if res.Header.Get("Last-Event-ID") != c.lastId {
		err = errors.New("Resume failed")
		c.lastId = ""
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

func (c *Consumer) loadLastEventID() (id string, err error) {
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
		id = string(content)
	}
	return
}

func (c *Consumer) saveLastEventID(id string) error {
	return ioutil.WriteFile(c.options.StateFile, []byte(id), 0644)
}
