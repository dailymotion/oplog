package consumer

import "sync"

type InFlightEvents struct {
	// ids is the list of in flight event IDs
	ids []string
	// mu is a mutex to protect ids from concurrent accesses
	mu *sync.RWMutex
}

// NewInFlightEvents contains events ids which have been received but not yet acked
func NewInFlightEvents() *InFlightEvents {
	return &InFlightEvents{
		ids: []string{},
		mu:  &sync.RWMutex{},
	}
}

// Count returns the number of events in flight.
func (ife *InFlightEvents) Count() int {
	ife.mu.RLock()
	defer ife.mu.RUnlock()
	return len(ife.ids)
}

// Push adds a new event id to the IFE
func (ife *InFlightEvents) Push(id string) {
	ife.mu.Lock()
	defer ife.mu.Unlock()

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
	ife.mu.Lock()
	defer ife.mu.Unlock()

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
