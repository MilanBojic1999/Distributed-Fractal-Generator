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
	Ration     float32            `json:"p"`
	Width      int                `json:"width"`
	Height     int                `json:"height"`
	MainPoints []structures.Point `json:"mainPoints"`
	Points     []structures.Point `json:"-"`
	working    bool               `json:"-"`
}

func (job *Job) MakeImage(path string) {

	red := color.RGBA{255, 0, 0, 0xff}

	img := image.NewRGBA(image.Rect(0, 0, job.Width, job.Height))

	draw.Draw(img, img.Bounds(), &image.Uniform{color.White}, image.ZP, draw.Src)

	for _, p := range job.Points {
		img.Set(p.X, p.Y, color.Black)
	}

	for _, p := range job.MainPoints {
		img.Set(p.X, p.Y, red)
	}

	f, _ := os.Create(fmt.Sprintf("%s/image_%s.png", path, job.Name))

	png.Encode(f, img)
}
