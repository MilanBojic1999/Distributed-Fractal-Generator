package job

import "distributed/structures"

type Job struct {
	Name       string             `json:"name"`
	PointCount int                `json:"pointCount"`
	Ration     int                `json:"p"`
	Width      int                `json:"width"`
	Height     int                `json:"height"`
	Points     []structures.Point `json:"-"`
}
