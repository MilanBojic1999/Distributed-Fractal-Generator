package job

import "distributed/structures"

type Job struct {
	Name       string             `json:"name"`
	PointCount int                `json:"pointCount"`
	Ration     float32            `json:"p"`
	Width      int                `json:"width"`
	Height     int                `json:"height"`
	Points     []structures.Point `json:"-"`
}
