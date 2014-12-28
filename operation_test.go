package oplog

import "testing"

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
