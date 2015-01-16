<?php
$operation = array( 
    "event" => "insert",
    "parents" => array(
        "user/1234"
    ),
    "type" => "video",
    "id" => "abcd",
);

$socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
$data = json_encode($operation);
socket_sendto($socket, $data, strlen($data), 0, '127.0.0.1', 8042);
socket_close($socket);

