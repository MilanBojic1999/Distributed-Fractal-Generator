package main

import (
	"distributed/bootstrap"
	"distributed/job"
	"distributed/structures"
	"distributed/worker"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

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

		JobList = ParseJobJson(jobs_interface)

		fmt.Printf("%T %v\n", jobs_interface, jobs_interface)
		worker.RunWorker(bootMap["ipAddress"].(string), int(bootMap["port"].(float64)), bootMap["bootstrapIpAddress"].(string), int(bootMap["bootstrapPort"].(float64)), JobList, *FILE_SEPARATOR)
	}
}

type JobStringed struct {
	Name       string             `json:"name"`
	PointCount int                `json:"pointCount"`
	Ration     string             `json:"ratio"`
	Width      int                `json:"width"`
	Height     int                `json:"height"`
	MainPoints []structures.Point `json:"mainPoints"`
	Points     []structures.Point `json:"-"`
}

func ParseJobJson(jobs_interface any) []job.Job {
	var JobList []job.Job
	var JobStrings []map[string]interface{}
	mapstructure.Decode(jobs_interface, &JobStrings)
	fmt.Printf("\nHj: %v\nHIhi: %v\n\n", jobs_interface, JobStrings)
	for _, v := range JobStrings {
		fmt.Printf("JOJO %v %v_%T\n", v, v["ratio"], v["ratio"])
	}
	JobList = make([]job.Job, 0, len(JobStrings))
	for _, v := range JobStrings {
		JobList = append(JobList, makeJob(v))
	}
	return JobList
}

func makeJob(j map[string]interface{}) job.Job {
	jobr := new(job.Job)
	jobr.Name = j["name"].(string)
	jobr.PointCount = int(j["pointCount"].(float64))
	jobr.Ration, _ = strconv.ParseFloat(j["ratio"].(string), 32)
	jobr.Width = int(j["width"].(float64))
	jobr.Height = int(j["height"].(float64))

	yoyo := j["mainPoints"].([]interface{})
	points := make([]structures.Point, 0, len(yoyo))
	for _, yy := range yoyo {
		var tt structures.Point
		mapstructure.Decode(yy, &tt)
		points = append(points, tt)
	}

	fmt.Printf("WWWW: %v\n", points)
	// jobr.MainPoints = make([]structures.Point, 0, len(points))
	jobr.MainPoints = points

	return *jobr
}
