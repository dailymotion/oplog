var operation = {
    "event": "create",
    "parents": [
        "user/1234"
    ],
    "type": "video",
    "id": "abcd",
}
var dgram = require('dgram');
var data = new Buffer(JSON.stringify(operation));
var client = dgram.createSocket('udp4');
client.send(data, 0, data.length, 8042, '127.0.0.1', function(err, bytes) {
    client.close();
});
