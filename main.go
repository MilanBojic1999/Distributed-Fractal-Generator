package main

import (
	"distributed/bootstrap"
	"distributed/job"
	"distributed/worker"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func check(e error, addition string) {
	if e != nil {
		// fmt.Println(e)
		fmt.Println(e.Error(), addition)
	}
}

func main() {

	FILE_SEPARATOR := flag.String("fileSeparator", "\\", "File seperator \\ for Windows (/ for Linux)")
	systemFile := flag.String("systemFile", "files"+*FILE_SEPARATOR+"system", "Path to file that describes system")
	isBootstrap := flag.Bool("bootstrap", false, "Is node a bootstrap")

	flag.Parse()

	var bootMap map[string]interface{}
	dat, err := os.ReadFile(*systemFile)
	if err != nil {
		fmt.Println(*systemFile)
		check(err, "parseMapString")
		return
	}
	json.Unmarshal(dat, &bootMap)

	if *isBootstrap {
		bootstrap.RunBootstrap(bootMap["ipAddress"].(string), bootMap["port"].(int), *FILE_SEPARATOR)
	} else {
		worker.RunWorker(bootMap["ipAddress"].(string), bootMap["port"].(int), bootMap["bootstrapIpAddress"].(string), bootMap["bootstrapPort"].(int), bootMap["jobs"].([]job.Job), *FILE_SEPARATOR)
	}
}
