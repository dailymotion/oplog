# OpLog

OpLog (for operation log) is an agent meant to be used as a data synchronization layer between a producer and consumers in a typical micro-services architecture. It can be seen as generic database replication system for web APIs.

A typical use-case is when a component handles an authoritative database, and several independent components needs to keep an up-to-date read-only view of the data locally (i.e.: search engine indexing, recommendation engines, multi-regions architecture, etc.) or to react on certain changes (i.e.: spam detection, analytics, etc.).

Another use-case is to implement a public streaming API to monitor objects changes on the service's model. With the use of [Server Sent Event](http://dev.w3.org/html5/eventsource/) and filtering, it might be used directly from the browser to monitor changes happening on objects shown on the page (à la [Meteor](https://www.meteor.com)).

The agent can run locally on every nodes of a cluster producing updates to the data store. The agent receives updates via UDP from the producer application and forward them to a central data store. If the central data store is not available, updates are buffered in memory.

The agent also exposes a [Server Sent Event](http://dev.w3.org/html5/eventsource/) API for consumers to be notified in real time about model changes. Thanks to the SSE protocol, a consumer can recover a connection breakage without loosing any updates.

A full replication is also supported for freshly spawned consumers that need to have a full view of the data.

Change metadata are stored on a central MongoDB server. A tailable cursor on capped collection is used for real time updates and final state of objects are also maintained in a secondary collection for full replication. The actual data is not stored in the oplog, the monitored API stays the authoritative source of data. Only modified object's `type` and `id` are stored together with the timestamp of the update and some related "parent" object references, useful for filtering. What you put in `type`, `id` and `parents` is up to the service, and must be meaningful to fetch the actual objects data from their API.

## Install

To install the project, execute the following commands:

    go get github.com/dailymotion/oplog
    go build -o /usr/local/bin/oplogd github.com/dailymotion/oplog/cmd/oplogd
    go build -o /usr/local/bin/oplog-sync github.com/dailymotion/oplog/cmd/oplog-sync
    go build -o /usr/local/bin/oplog-tail github.com/dailymotion/oplog/cmd/oplog-tail

## Starting the agent

To start the agent, run the following command:

    oplogd --mongo-url mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]]/database[?options]

The `oplog` and `objects` collections will be created in the specified database.

## UDP API

To send operation events to the agent, an UDP datagram containing a JSON object must be crafted and sent on the agent's UDP port (8042 by default).

The format of the message is as follow:

```javascript
{
    "event": "insert",
    "parents": ["video/xk32jd", "user/xkjdi"],
    "type": "video",
    "id": "xk32jd",
}
```

The following keys are required:

* `event`: The type of event. Can be `insert`, `update` or `delete`.
* `parents`: The list of parent objects of the modified object. The advised format for items of this list is `type/id` but any format is acceptable. It is generally a good idea to put a reference to the modified object itself in this list in order to easily let the consumers filter on any updates performed on the object.
* `type`: The object type (i.e.: `video`, `user`, `playlist`, …)
* `id`: The object id of the impacted object as string.

See `examples/` directory for implementation examples in different languages.

## Server Sent Event API

The [SSE](http://dev.w3.org/html5/eventsource/) API runs on the same port as UDP API but in TCP. It means that agents have both input and output roles so it is easy to scale the service by putting an agent on every nodes of the source API cluster and expose their HTTP port via the same load balancer as the API while each nodes can send their updates to the UDP port on their localhost.

The W3C SSE protocol is respected by the book. To connect to the API, a GET on `/` with the `Accept: text/event-stream` header is performed. If no `Last-Event-ID` HTTP header is passed, the oplog server will start sending all future events with no backlog. On each received event, the client must store the last event id as they are treated and submit it back to the server on reconnect using the `Last-Event-ID` HTTP header in order to resume the transfer where it has been interrupted.

The client must ensure the `Last-Event-ID` header is sent back in the response. It may happen that the id defined by `Last-Event-ID` is no longer available in the underlaying capped collection. In such case, the agent won't send the backlog as if the `Last-Event-ID` header hadn't been sent. The requested `Last-Event-ID` header won't be sent back in the response either. You may want to perform a full replication when this happen.

The following filters can be passed as query-string:
* `types` A list of object types to filter on separated by comas (i.e.: `types=video,user`).
* `parents` A coma separated list of parents to filter on (i.e.: `parents=video/xk32jd,user/xkjdi`

```
GET / HTTP/1.1
Accept: text/event-stream

HTTP/1.1 200 OK
Content-Type: text/event-stream; charset=utf-8

id: 545b55c7f095528dd0f3863c
event: insert
data: {"timestamp":"2014-11-06T03:04:39.041-08:00","parents":["x3kd2"],"type":"video","id":"xekw"}

id: 545b55c8f095528dd0f3863d
event: delete
data: {"timestamp":"2014-11-06T03:04:40.091-08:00","parents":["x3kd2"],"type":"video","id":"xekw"}

…
```

## Full Replication

If required, a full replication with all (not deleted) objects can be performed before streaming live updates. To perform a full replication, pass `0` as `Last-Event-ID`. Numeric event ids with 13 digits or less are considered as a replication id, which represent a milliseconds UNIX timestamp. By passing a millisecond timestamp, you are asking for replicating any objects that have been modified after this date. Passing `0` thus ensures every objects will be replicated.

If a full replication is interrupted during the transfer, the same mechanism as for live updates is used. Once replication is done, the stream will automatically switch to live events stream so the consumer is ensured not to miss any updates.

When a full replication starts, a special `reset` event is sent to signify the consumer that it should reset its database before applying the subsequent operations.

Once the replication is done and the oplog switch back to the live updates, a special `live` event is sent. This event can be useful for a consumer to know when it is safe to be activated in production.

## Periodical Source Synchronization

There is many ways for the oplog to miss some updates and thus have an incorrect view of the current state of the source data. In order to cope with this issue, a regular synchronization process with the source data content can be performed. The sync is a separate process which compares a dump of the real data with what the oplog have stored in its own database. For any discrepancies **which is anterior** to the dump in the oplog's database, the process will generate an appropriate event in the oplog to fix the delta on both its own database as well as for all consumers.

The dump must be in a streamable JSON format. Each line is a JSON object with the same schema as of the `data` part of the SEE API response.
Dump example:

```javascript
{"timestamp":"2014-11-06T03:04:39.041-08:00", "parents": ["user/xl2d"], "type":"video", "id":"x34cd"}
{"timestamp":"2014-12-24T02:03:05.167+01:00", "parents": ["user/xkwek"], "type":"video", "id":"x12ab"}
{"timestamp":"2014-12-24T01:03:05.167Z", "parents": ["user/xkwek"], "type":"video", "id":"x54cd"}
…
```

The `timestamp` must represent the last modification date of the object as an RFC 3339 representation.

The `oplog-sync` command is used with this dump in order to perform the sync. This command will connect to the database, do the comparisons and generate the necessary oplog events to fix the deltas.


## Status Endpoint

The agent exposes a `/status` endpoint over HTTP to show some statistics about the itself. A JSON object is returned with the following fields:

* `events_received`: Total number of events received on the UDP interface
* `events_ingested`: Total number of events ingested into MongoDB with success
* `events_error`: Total number of events received on the UDP interface with an invalid format
* `events_discarded`: Total number of events discarded because the queue was full
* `queue_size`: Current number of events in the ingestion queue
* `queue_max_size`:  Maximum number of events allowed in the ingestion queue before discarding events
* `clients`: Number of clients connected to the SSE API

```javascript
GET /status

HTTP/1.1 200 OK
Content-Length: 144
Content-Type: application/json
Date: Thu, 06 Nov 2014 10:40:25 GMT

{
    "clients": 0,
    "events_discarded": 0,
    "events_error": 0,
    "events_ingested": 0,
    "events_received": 0,
    "queue_max_size": 100000,
    "queue_size": 0,
    "status": "OK"
}
```

## Consumer

To write a consumer you may use any SSE library and consume the API by ourself. If your consumer is written in Go, a dedicated consumer library is provided.

Here is an example of Go consumer using the provided consumer library:

```go
import (
    "fmt"

    "github.com/dailymotion/oplog/consumer"
)

func main() {
    c, err := consumer.Subscribe(myOplogURL, consumer.Options{})

    if err != nil {
        log.Fatal(err)
    }

    ops := make(chan consumer.Operation)
    ack := make(chan consumer.Operation)
    go c.Process(ops, ack)

    for {
        // Get the next operation
        op := <-ops

        // Do something with the operation
        url := fmt.Sprintf("http://api.domain.com/%s/%s", op.Data.Type, op.Data.ID)
        data := MyAPIGetter(url)
        MyDataSyncer(data)

        // Ack the fact you handled the operation
        ack <- op
    }
}
```

The ack mechanism allows you to handle operation in parallel without loosing track of which operation has been handled in case of a connection failure recovery.

See `cmd/oplog-tail/` for another usage example.
