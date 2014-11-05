var server_address = 'push-02.adm.dailymotion.com';
var event_name = 'user.postVideo.x42k6o';
var data = {
  'msg': '${var.sender} published ${var.target}.',
  'msg.fr': '${var.sender} a publié ${var.target}.',
  'msg.it': '${var.sender} ha pubblicato ${var.target}.',
  'msg.de': '${var.sender} veröffentlicht ${var.target}.',
  'var.sender': 'akh',
  'var.target': 'c\'est deeeuuuu laaa meeeeeerde!',
  'data.tile': 'L3VzZXIvcmVuMDIyNC8x',
  'data.video': 'xy65zq',
  'data.video_title': 'ホドン&チャンミンのMoonlight プリンス #1-2',
  'data.video_thumbnail_url': 'http://static2.dmcdn.net/static/video/646/493/57394646:jpeg_preview_large.jpg?20130313162706',
  'data.sender_name': 'ren0224',
  'data.sender_screenname': 'ren0224',
  'data.sender_image_url': 'http://static2.dmcdn.net/static/user/661/606/75606166:avatar_large.jpg?20121027114320'
};

var dgram = require('dgram');
var zlib = require('zlib');
var querystring = require('querystring');
var message = new Buffer('/event/' + event_name + '?' + querystring.stringify(data));
zlib.gzip(message, function(err, message) {
  var client = dgram.createSocket('udp4');
  client.send(message, 0, message.length, 80, server_address, function(err, bytes) {
    client.close();
  });
});
