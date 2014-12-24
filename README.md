# OpLog

OpLog (for operation log) is a Go agent meant to be used as a data synchronization layer between a producer component and one or more consumer components in a typical [SOA](http://en.wikipedia.org/wiki/Service-oriented_architecture) architecture. It can be seen as generic database replication system at the application logic layer.

A typical use-case is when a central component handles the central authoritative database, and several independent micro-components needs to keep an up-to-date read-only view of the data locally for performance purpose (i.e.: in multi-regions architecture).

The agent runs locally on every hosts of a cluster generating updates to a data store, and listen for UDP updates from the component describing every changes happening on the models.

The agent then exposes an [Server Sent Event](http://dev.w3.org/html5/eventsource/) API for consumers to be notified in real time about model changes. Thanks to the SSE protocol, a consumer can recover a connection breakage without loosing any update.

A full replication is also supported for freshly spawned consumers that need to initialize their database from scratch.

## Install

Because this repository is currently private, you may have to make sure git will use github using ssh if you are using 2FA. You can set this up with the following command:

    git config --global url."git@github.com:".insteadOf "https://github.com/"

Then to install the project, execute the following two commands:

    go get github.com/dailymotion/oplog
    go build -o /usr/local/bin/oplogd github.com/dailymotion/oplog/cmd/oplogd
    go build -o /usr/local/bin/oplog-sync github.com/dailymotion/oplog/cmd/oplog-sync

## Starting the agent

To start the agent, run the following command:

    oplogd --mongo-url mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]]/database[?options]

The `oplog` and `objects` collection will be created in the provided database.

## UDP API

To send operation events to the agent, an UDP datagram containing a JSON object must be crafted and sent on the agent's UDP port (8042 by default).

The format of the message is as follow:

```javascript
{
    "event": "insert",
    "parents": ["user/x1234"],
    "type": "video",
    "id": "x345",
}
```

All keys are required:

* `event`: The type of event. Can be `insert`, `update` or `delete`.
* `parents`: The list of parent objects of the modified object formated as `type/id`.
* `type`: The object type (i.e.: `video`, `user`, `playlist`…)
* `id`: The object id of the impacted object as string.

## Server Sent Event API

The SSE API runs on the same port as UDP API but using TCP. The W3C SSE protocol is respected by the book. To connect to the API, a GET on `/` with the `Accept: text/event-stream` header is performed.

On each received event, the client must store the last event id and submit it back to the server on reconnect using the `Last-Event-ID` HTTP header in order to resume the transfer where it has been left. The client must then ensure the `Last-Event-ID` header is sent back in the response. It may happen that the id defined by `Last-Event-ID` is no longer available, in this case, the agent won't send the backlog and will ignore the `Last-Event-ID` header. You may want to perform a full replication in such a case.

The following filters can be passed as query-string:
* `types` A list of object types to filter on separated by comas (i.e.: `types=video,user`).
* `parents` A coma separated list of `type/id` to filter on

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

## Periodical Source Synchronization

There is many ways for the oplog to miss some updates and thus have an incorrect view of the current state of the source database. In order to cope with this issue, a regular synchronization process with the source database content can be performed. The sync is a separate process which compares a dump of the real database with what the oplog have stored in its own database. For any discrepancies which is anterior to the dump in the oplog's db, the process will generate an appropriate event in the oplog to fix the delta.

The sync process is performed in two phases:

1. The first phase look for every objects present in the dump, and try to find the same item in the oplog database. If the object is missing or is flagged as deleted, a `create` event is generated to rectify the oplog db status and notify the consumers.
2. The second phase search for any item present in the oplog database and not marked as deleted but not present in the dump and generate a `delete` event for them.

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


