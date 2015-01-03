package consumer_test

import (
	"log"

	"github.com/dailymotion/oplog/consumer"
)

func Example() {
	myOplogURL := "http://oplog.mydomain.com"
	c := consumer.Subscribe(myOplogURL, consumer.Options{})

	ops := make(chan consumer.Operation)
	errs := make(chan error)
	done := make(chan bool)
	go c.Process(ops, errs, done)

	for {
		select {
		case op := <-ops:
			// Got the next operation
			switch op.Event {
			case "reset":
				// reset the data store
			case "live":
				// put the service back in production
			default:
				// Do something with the operation
				//url := fmt.Sprintf("http://api.domain.com/%s/%s", op.Data.Type, op.Data.ID)
				//data := MyAPIGetter(url)
				//MyDataSyncer(data)
			}

			// Ack the fact you handled the operation
			op.Done()
		case err := <-errs:
			switch err {
			case consumer.ErrAccessDenied, consumer.ErrWritingState:
				c.Stop()
				log.Fatal(err)
			case consumer.ErrResumeFailed:
				log.Print("Resume failed, forcing full replication")
				c.SetLastId("0")
			default:
				log.Print(err)
			}
		case <-done:
			return
		}
	}
}
