package oplog

import "testing"

// parseObjectId()

func TestParseInvalidObjectId(t *testing.T) {
	if parseObjectId("1419043454520") != nil {
		t.Fail()
	}
	if parseObjectId("zzzzzzzzzzzzzzzzzzzzzzzz") != nil {
		t.Fail()
	}
}

func TestParseEmptyObjectId(t *testing.T) {
	if parseObjectId("") != nil {
		t.Fail()
	}
}

func TestParseValidObjectId(t *testing.T) {
	if parseObjectId("545b4f8ef095528dd0f3863b") == nil {
		t.Fail()
	}
}

// parseTimestampId()

func TestParseInvalidTimestamp(t *testing.T) {
	if _, ok := parseTimestampId("141904345452a"); ok {
		t.Fail()
	}
	if _, ok := parseTimestampId("141904345452014190434545"); ok {
		t.Fail()
	}
	if _, ok := parseTimestampId("141904345452014190434545"); ok {
		t.Fail()
	}
}

func TestParseValidTimestamp(t *testing.T) {
	if _, ok := parseTimestampId("1419043454520"); !ok {
		t.Fail()
	}
}

func TestParseZeroTimestamp(t *testing.T) {
	if _, ok := parseTimestampId("0"); !ok {
		t.Fail()
	}
}

// NewLastId()

func TestNewLastIdEmtpyString(t *testing.T) {
	_, err := NewLastId("")
	if err == nil {
		t.Fail()
	}
}

func TestNewLastIdInvalid(t *testing.T) {
	_, err := NewLastId("abcd")
	if err == nil {
		t.Fail()
	}
}

func TestNewLastIdObjectId(t *testing.T) {
	i, err := NewLastId("54e07b75f2fcd8c74bb7bad3")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := i.(*OperationLastId); !ok {
		t.Fail()
	}
}

func TestNewLastIdZero(t *testing.T) {
	i, err := NewLastId("0")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := i.(*ReplicationLastId); !ok {
		t.Fail()
	}
}

func TestNewLastIdTimestamp(t *testing.T) {
	i, err := NewLastId("1423995187898")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := i.(*ReplicationLastId); !ok {
		t.Fail()
	}
}

// String

func TestNewLastIdTimestampString(t *testing.T) {
	i, _ := NewLastId("1423995187898")
	if i.(*ReplicationLastId).int64 != 1423995187898 {
		t.Fail()
	}
}

func TestNewLastIdOperationString(t *testing.T) {
	i, _ := NewLastId("54e07b75f2fcd8c74bb7bad3")
	if i.(*OperationLastId).ObjectId.Hex() != "54e07b75f2fcd8c74bb7bad3" {
		t.Fail()
	}
}

// Fallback

func TestFallbackOperation(t *testing.T) {
	i, err := NewLastId("54e07b75f2fcd8c74bb7bad3")
	if err != nil {
		t.Fatal(err)
	}
	r := i.(*OperationLastId).Fallback()
	if _, ok := r.(*ReplicationLastId); !ok {
		t.FailNow()
	}
	if r.(*ReplicationLastId).int64 != 1423997813000 {
		t.Fail()
	}
}
