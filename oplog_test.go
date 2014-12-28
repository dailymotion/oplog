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

// Operation.Validate()

func TestOperationValidate(t *testing.T) {
	op := Operation{
		Event: "create",
		Data: &OperationData{
			Id:   "id",
			Type: "type",
		},
	}
	if err := op.Validate(); err != nil {
		t.Fail()
	}
	op.Event = "update"
	if err := op.Validate(); err != nil {
		t.Fail()
	}
	op.Event = "delete"
	if err := op.Validate(); err != nil {
		t.Fail()
	}
}

func TestOperationValidateInvalidEventName(t *testing.T) {
	op := Operation{
		Event: "invalid",
		Data: &OperationData{
			Id:   "id",
			Type: "type",
		},
	}
	if err := op.Validate(); err == nil {
		t.Fail()
	}
}

// OperationData.Validate()

func TestOperationDataValidate(t *testing.T) {
	opd := OperationData{
		Id:      "id",
		Type:    "type",
		Parents: []string{"parent/id"},
	}
	if err := opd.Validate(); err != nil {
		t.Fail()
	}
}

func TestOperationDataValidateEmptyId(t *testing.T) {
	opd := OperationData{
		Id:   "",
		Type: "type",
	}
	if err := opd.Validate(); err == nil {
		t.Fail()
	}
}

func TestOperationDataValidateEmptyType(t *testing.T) {
	opd := OperationData{
		Id:   "id",
		Type: "",
	}
	if err := opd.Validate(); err == nil {
		t.Fail()
	}
}

func TestOperationDataValidateEmptyParentItem(t *testing.T) {
	opd := OperationData{
		Id:      "id",
		Type:    "type",
		Parents: []string{""},
	}
	if err := opd.Validate(); err == nil {
		t.Fail()
	}
}
