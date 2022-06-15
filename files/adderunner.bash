#!/bin/bash


LOCAL_ADDRESS="172.24.54.97"
BASE_PORT=9300

maxim=3
for (( i=0; i < $maxim; ++i ))
do
    go run main.go --fileSeperator / --systemFile files/system/worker1.json --IpAddress $LOCAL_ADDRESS --Port $(($i + $BASE_PORT)) --BootstrapIpAddress $LOCAL_ADDRESS  &
    sleep 5
done