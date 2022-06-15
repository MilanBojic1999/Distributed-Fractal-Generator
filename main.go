package main

import (
	"distributed/bootstrap"
	"distributed/job"
	"distributed/structures"
	"distributed/worker"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

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

	ipAddressFlag := flag.String("IpAddress", "", "dynamic ip address")
	portFlag := flag.String("Port", "", "dynamic port")
	bootstrapIpAddressFlag := flag.String("BootstrapIpAddress", "", "bootstrap ip address")
	bootstrapPortFlag := flag.String("BootstrapPort", "", "bootstrap port")
	listenCommandFlag := flag.Bool("Listener", false, "Node listen to CLI")

	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	var bootMap map[string]interface{}
	dat, err := os.ReadFile(*systemFile)
	if err != nil {
		fmt.Println(*systemFile)
		check(err, "parseMapString")
		return
	}

	json.Unmarshal(dat, &bootMap)

	var ipAddress, bootstrapIpAddress string

	var port, bootstrapPort int

	if len(*ipAddressFlag) > 0 {
		ipAddress = *ipAddressFlag
	} else {
		ipAddress = bootMap["ipAddress"].(string)
	}

	if len(*portFlag) > 0 {
		port, _ = strconv.Atoi(*portFlag)
	} else {
		port = int(bootMap["port"].(float64))
	}

	fmt.Println(bootMap)
	fmt.Println(ipAddress, bootstrapIpAddress, port, bootstrapPort)

	if *isBootstrap {
		bootstrap.RunBootstrap(ipAddress, port, *FILE_SEPARATOR, *listenCommandFlag)
	} else {

		if len(*bootstrapIpAddressFlag) > 0 {
			bootstrapIpAddress = *bootstrapIpAddressFlag
		} else {
			bootstrapIpAddress = bootMap["bootstrapIpAddress"].(string)
		}

		if len(*bootstrapPortFlag) > 0 {
			bootstrapPort, _ = strconv.Atoi(*bootstrapPortFlag)
		} else {
			bootstrapPort = int(bootMap["bootstrapPort"].(float64))
		}

		var JobList []job.Job
		jobs_interface := bootMap["jobs"]

		JobList = ParseJobJson(jobs_interface)

		// mapstructure.Decode(jobs_interface, JobList)

		fmt.Printf("%T %v\n", jobs_interface, jobs_interface)
		worker.RunWorker(ipAddress, port, bootstrapIpAddress, bootstrapPort, JobList, *FILE_SEPARATOR, *listenCommandFlag)
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
	tmp_float, _ := strconv.ParseFloat(j["ratio"].(string), 32)
	jobr.Ratio = structures.MyFloat(tmp_float)
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
