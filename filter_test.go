package oplog

import (
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func TestFilterSingleType(t *testing.T) {
	q := bson.M{}
	f := OpLogFilter{Types: []string{"a"}}
	f.apply(&q)
	if q["data.t"] != "a" {
		t.Fail()
	}
}

func TestFilterMultiTypes(t *testing.T) {
	q := bson.M{}
	f := OpLogFilter{Types: []string{"a", "b"}}
	f.apply(&q)
	m, ok := q["data.t"].(bson.M)
	if !ok {
		t.Fatal("data.t is not a sub-bson")
	}
	s, ok := m["$in"].([]string)
	if !ok {
		t.Fatal("data.t doesn't contain a $in")
	}
	if s[0] != "a" || s[1] != "b" {
		t.FailNow()
	}
}

func TestFilterSingleParent(t *testing.T) {
	q := bson.M{}
	f := OpLogFilter{Parents: []string{"a"}}
	f.apply(&q)
	if q["data.p"] != "a" {
		t.Fail()
	}
}

func TestFilterMultiParents(t *testing.T) {
	q := bson.M{}
	f := OpLogFilter{Parents: []string{"a", "b"}}
	f.apply(&q)
	m, ok := q["data.p"].(bson.M)
	if !ok {
		t.Fatal("data.p is not a sub-bson")
	}
	s, ok := m["$in"].([]string)
	if !ok {
		t.Fatal("data.p doesn't contain a $in")
	}
	if s[0] != "a" || s[1] != "b" {
		t.FailNow()
	}
}
