package main

import (
	"distributed/bootstrap"
	"distributed/job"
	"distributed/worker"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/mitchellh/mapstructure"
)

func check(e error, addition string) {
	if e != nil {
		// fmt.Println(e)
		fmt.Println(e.Error(), addition)
	}
}

func main() {

	FILE_SEPARATOR := flag.String("fileSeperator", "\\", "File seperator \\ for Windows (/ for Linux)")
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

	fmt.Println(bootMap)

	if *isBootstrap {
		bootstrap.RunBootstrap(bootMap["ipAddress"].(string), int(bootMap["port"].(float64)), *FILE_SEPARATOR)
	} else {

		var JobList []job.Job
		jobs_interface := bootMap["jobs"]
		mapstructure.Decode(jobs_interface, &JobList)
		fmt.Println(JobList)
		worker.RunWorker(bootMap["ipAddress"].(string), int(bootMap["port"].(float64)), bootMap["bootstrapIpAddress"].(string), int(bootMap["bootstrapPort"].(float64)), JobList, *FILE_SEPARATOR)
	}
}
