package oplog

import "gopkg.in/mgo.v2/bson"

// Filter contains filter query
type Filter struct {
	Types   []string
	Parents []string
}

// Apply applies the filters to the given query
func (f Filter) apply(query *bson.M) {
	switch len(f.Types) {
	case 0:
		// Do nothing
	case 1:
		(*query)["data.t"] = f.Types[0]
	default: // > 1
		(*query)["data.t"] = bson.M{"$in": f.Types}
	}

	switch len(f.Parents) {
	case 0:
		// Do nothing
	case 1:
		(*query)["data.p"] = f.Parents[0]
	default: // > 1
		(*query)["data.p"] = bson.M{"$in": f.Parents}
	}
}
