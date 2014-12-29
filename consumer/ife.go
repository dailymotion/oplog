package consumer

import "sync"

type InFlightEvents struct {
	sync.RWMutex
	// ids is the list of in flight event IDs
	ids []string
}

// NewInFlightEvents contains events ids which have been received but not yet acked
func NewInFlightEvents() *InFlightEvents {
	return &InFlightEvents{
		ids: []string{},
	}
}

// Count returns the number of events in flight.
func (ife *InFlightEvents) Count() int {
	ife.RLock()
	defer ife.RUnlock()
	return len(ife.ids)
}

// Push adds a new event id to the IFE
func (ife *InFlightEvents) Push(id string) {
	ife.Lock()
	defer ife.Unlock()

	for _, eid := range ife.ids {
		if eid == id {
			// do not push the id if already in
			return
		}
	}

	ife.ids = append(ife.ids, id)
}

// Pull pulls the given id from the list if found and set a boolean as true
// if it is the first element of the list.
func (ife *InFlightEvents) Pull(id string) (found bool, first bool) {
	ife.Lock()
	defer ife.Unlock()

	for idx, eid := range ife.ids {
		if eid == id {
			found = true
			if idx == 0 {
				first = true
			}
			ife.ids = append(ife.ids[:idx], ife.ids[idx+1:]...)
			break
		}
	}

	return
}
