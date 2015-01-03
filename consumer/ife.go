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

// Pull pulls the given id from the list and returns the index
// of the pulled element in the queue. If the element wasn't found
// the index is set to -1.
func (ife *InFlightEvents) Pull(id string) (index int) {
	ife.Lock()
	defer ife.Unlock()
	index = -1

	for i, eid := range ife.ids {
		if eid == id {
			index = i
			ife.ids = append(ife.ids[:i], ife.ids[i+1:]...)
			break
		}
	}

	return
}
