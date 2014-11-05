var data = {
    "event": "INSERT",
    "user_id": "x1234",
    "type": "video",
    "id": "x345",
}
var dgram = require('dgram');
var message = new Buffer(JSON.stringify(data));
var client = dgram.createSocket('udp4');
client.send(message, 0, message.length, 8042, '127.0.0.1', function(err, bytes) {
    client.close();
});
