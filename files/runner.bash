#!/bin/bash

go run main.go --bootstrap --fileSeperator / --systemFile files/system/bootstrap.json
sleep 2

go run main.go --fileSeperator / --systemFile files/system/worker1.json
sleep 4
go run main.go --fileSeperator / --systemFile files/system/worker2.json
