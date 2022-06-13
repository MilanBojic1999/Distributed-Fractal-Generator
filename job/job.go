package job

import (
	"distributed/structures"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os"
)

type Job struct {
	Name       string             `json:"name"`
	PointCount int                `json:"pointCount"`
	Ration     float64            `json:"ratio"`
	Width      int                `json:"width"`
	Height     int                `json:"height"`
	MainPoints []structures.Point `json:"mainPoints"`
	Points     []structures.Point `json:"allPoints"`
	Working    bool               `json:"-"`
}

func (job *Job) Log() string {
	return fmt.Sprintf("Job %s: [%d %f] Resolution: %d x %d", job.Name, job.PointCount, job.Ration, job.Height, job.Width)
}

func (job *Job) MakeImage(path string) {

	red := color.RGBA{255, 0, 0, 0xff}

	img := image.NewRGBA(image.Rect(0, 0, job.Width, job.Height))

	draw.Draw(img, img.Bounds(), &image.Uniform{color.White}, image.Point{}, draw.Src)

	fmt.Printf("Number of new points: %d\n", len(job.Points))

	for _, p := range job.Points {
		img.Set(p.X, p.Y, color.Black)
	}

	fmt.Println(job.MainPoints)

	for _, p := range job.MainPoints {
		img.Set(p.X, p.Y, red)
	}

	f, _ := os.Create(fmt.Sprintf("%s/image_%s.png", path, job.Name))

	png.Encode(f, img)
}

type JobStatus struct {
	Name            string         `json:"name"`
	PointsGenerated int            `json:"pointsGenerated"`
	WorkingNodes    int            `json:"workingNodes"`
	PointsPerNodes  map[string]int `json:"pointNodes"`
}

func (js *JobStatus) Log() string {
	return fmt.Sprintf("Job Status %s, with %d gen points and %d working nodes", js.Name, js.PointsGenerated, js.WorkingNodes)
}

func (j *Job) GetJobStatus(fractalID string) *JobStatus {
	jobStatus := new(JobStatus)
	jobStatus.Name = j.Name
	jobStatus.PointsGenerated = len(j.Points)
	jobStatus.WorkingNodes = 1
	jobStatus.PointsPerNodes = make(map[string]int)
	jobStatus.PointsPerNodes[fractalID] = jobStatus.PointsGenerated

	return jobStatus
}
