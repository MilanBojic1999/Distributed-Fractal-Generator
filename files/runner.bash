#!/bin/bash

LOCAL_ADDRESS="127.0.0.1"
BASE_PORT=6300

go run main.go --bootstrap --fileSeperator / --systemFile files/system/bootstrap.json --IpAddress $LOCAL_ADDRESS --Listener &
sleep 5

maxim=5
for (( i=0; i < $maxim; ++i ))
do
    go run main.go --fileSeperator / --systemFile files/system/worker1.json --IpAddress $LOCAL_ADDRESS --Port $(($i + $BASE_PORT)) --BootstrapIpAddress $LOCAL_ADDRESS  &
    sleep 2
done