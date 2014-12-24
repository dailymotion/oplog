package consumer

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

type Decoder struct {
	*bufio.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{bufio.NewReader(r)}
}

// Next reads the next operation from a stream or block until one comes in.
func (d *Decoder) Next(op *Operation) (err error) {
	var line string
	for {
		line, err = d.ReadString('\n')
		if err != nil {
			return
		}
		if line == "\n" {
			break
		}
		line = strings.TrimSuffix(line, "\n")
		if strings.HasPrefix(line, ":") {
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
				return
			}
		}
	}

	return
}
