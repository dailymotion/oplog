package oplog

import "testing"

// Version of bytes.Buffer that checks whether WriteTo was called or not
type writeChecker struct {
	written []byte
	called  bool
}

func (wt *writeChecker) Write(b []byte) (int, error) {
	wt.written = b
	wt.called = true
	return len(b), nil
}

func TestOplogEventOutput(t *testing.T) {
	e := OpLogEvent{"a", "b"}
	w := &writeChecker{}
	n, err := e.WriteTo(w)
	if err != nil {
		t.Fatal(err)
	}
	if !w.called {
		t.Fatal("writer not called")
	}
	if string(w.written) != "id: a\nevent: b\n\n" {
		t.Fatalf("invalid output: %s", string(w.written))
	}
	if n != int64(len(w.written)) {
		t.Fatalf("returned length doesn't match written string length: %d != %d", n, len(w.written))
	}
}

func TestOplogEventId(t *testing.T) {
	e := OpLogEvent{"a", "b"}
	if e.GetEventId().String() != "a" {
		t.FailNow()
	}
}
