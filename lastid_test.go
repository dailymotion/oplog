package oplog

import "testing"

// parseObjectID()

func TestParseInvalidObjectId(t *testing.T) {
	if parseObjectID("1419043454520") != nil {
		t.Fail()
	}
	if parseObjectID("zzzzzzzzzzzzzzzzzzzzzzzz") != nil {
		t.Fail()
	}
}

func TestParseEmptyObjectId(t *testing.T) {
	if parseObjectID("") != nil {
		t.Fail()
	}
}

func TestParseValidObjectId(t *testing.T) {
	if parseObjectID("545b4f8ef095528dd0f3863b") == nil {
		t.Fail()
	}
}

// parseTimestampID()

func TestParseInvalidTimestamp(t *testing.T) {
	if _, ok := parseTimestampID("141904345452a"); ok {
		t.Fail()
	}
	if _, ok := parseTimestampID("141904345452014190434545"); ok {
		t.Fail()
	}
	if _, ok := parseTimestampID("141904345452014190434545"); ok {
		t.Fail()
	}
}

func TestParseValidTimestamp(t *testing.T) {
	if _, ok := parseTimestampID("1419043454520"); !ok {
		t.Fail()
	}
}

func TestParseZeroTimestamp(t *testing.T) {
	if _, ok := parseTimestampID("0"); !ok {
		t.Fail()
	}
}

// NewLastID()

func TestNewLastIDEmtpyString(t *testing.T) {
	_, err := NewLastID("")
	if err == nil {
		t.Fail()
	}
}

func TestNewLastIDInvalid(t *testing.T) {
	_, err := NewLastID("abcd")
	if err == nil {
		t.Fail()
	}
}

func TestNewLastIDObjectId(t *testing.T) {
	i, err := NewLastID("54e07b75f2fcd8c74bb7bad3")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := i.(*OperationLastID); !ok {
		t.Fail()
	}
}

func TestNewLastIDZero(t *testing.T) {
	i, err := NewLastID("0")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := i.(*ReplicationLastID); !ok {
		t.Fail()
	}
}

func TestNewLastIDTimestamp(t *testing.T) {
	i, err := NewLastID("1423995187898")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := i.(*ReplicationLastID); !ok {
		t.Fail()
	}
}

// String

func TestNewLastIDTimestampString(t *testing.T) {
	i, _ := NewLastID("1423995187898")
	if i.(*ReplicationLastID).int64 != 1423995187898 {
		t.Fail()
	}
}

func TestNewLastIDOperationString(t *testing.T) {
	i, _ := NewLastID("54e07b75f2fcd8c74bb7bad3")
	if i.(*OperationLastID).ObjectId.Hex() != "54e07b75f2fcd8c74bb7bad3" {
		t.Fail()
	}
}

// Fallback

func TestFallbackOperation(t *testing.T) {
	i, err := NewLastID("54e07b75f2fcd8c74bb7bad3")
	if err != nil {
		t.Fatal(err)
	}
	r := i.(*OperationLastID).Fallback()
	if _, ok := r.(*ReplicationLastID); !ok {
		t.FailNow()
	}
	if r.(*ReplicationLastID).int64 != 1423997813000 {
		t.Fail()
	}
}
