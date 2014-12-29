package consumer

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

type Decoder struct {
	*bufio.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{bufio.NewReader(r)}
}

// Next reads the next operation from a SSE stream or block until one comes in.
func (d *Decoder) Next(op *Operation) (err error) {
	// Reset non reusable fields
	op.Event = ""
	op.Data = nil

	var line string

	for {
		if line, err = d.ReadString('\n'); err != nil {
			break
		}
		if line == "\n" {
			// Message is complete, send it
			return
		}
		line = strings.TrimSuffix(line, "\n")
		if strings.HasPrefix(line, ":") {
			// Comment, ignore
			continue
		}
		sections := strings.SplitN(line, ":", 2)
		field, value := sections[0], ""
		if len(sections) == 2 {
			value = strings.TrimPrefix(sections[1], " ")
		}
		switch field {
		case "id":
			op.ID = value
		case "event":
			op.Event = value
		case "data":
			// The oplog does never return data on serveral lines
			if err = json.Unmarshal([]byte(value), &op.Data); err != nil {
				break
			}
		}
	}

	err = errors.New("Incomplete event")
	return
}
