# Dailymotion Operation Log

The Dailymotion OpLog is a Go agent meant to run on every PHP server, listening for UDP commands from the website describing every changes happening on a Dailymotion object.

The agent exposes an [Server Sent Event](http://dev.w3.org/html5/eventsource/) API for external consumers to be notified in real time about model changes.

For more information, see the [Wiki page](https://wiki.dailymotion.com/display/XP/OpLog) about this project.

## UDP API

To send operation events to the agent, an UDP datagram a JSON object must be crafted and sent on the agent's UDP port (8042 by default).

The format of the message is as follow:

```javascript
{
    "event": "INSERT",
    "user_id": "x1234",
    "type": "video",
    "id": "x345",
}
```

All keys are required:

* `event`: The type of event. Can be `INSERT`, `UPDATE` or `DELETE`.
* `user_id`: The owner xid of the modified object.
* `type`: The object type (i.e.: `video`, `user`, `playlist`…)
* `id`: The object xid of the impacted object.

Only xid must be used, numerical ids aren't accepted.

## SSE API

The SSE API runs on the same port as UDP API but using TCP. The W3C SSE protocol is respected by the book. To connect to the API, the `Accept: text/event-stream` header must be sent.

On each received event, the client must store the last event id and submit it back to the server on reconnect using the `Last-Event-ID` HTTP header. The client must then ensure the `Last-Event-ID` header is sent back in the response. It may happen that the id defined by `Last-Event-ID` is no longer available, in this case, the agent won't send the backlog and will ignore the `Last-Event-ID` header.
