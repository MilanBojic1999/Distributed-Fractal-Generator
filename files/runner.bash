#!/bin/bash

go run main.go --bootstrap --fileSeperator / --systemFile files/system/bootstrap.json &
sleep 0.5

go run main.go --fileSeperator / --systemFile files/system/worker1.json &
sleep 1
go run main.go --fileSeperator / --systemFile files/system/worker2.json &
# sleep 4
# go run main.go --fileSeperator / --systemFile files/system/worker3.json
