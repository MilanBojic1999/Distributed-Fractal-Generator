package node

import (
	"distributed/structures"
	"fmt"
)

type INode interface {
	GetAdders() string
	GetPort() int
	GetFullAddress() string
	GetType() string
	GetId() int
}

type Bootstrap struct {
	IpAddress string `json:"ipAddress"`
	Port      int    `json:"port"`
}

func (b *Bootstrap) GetAdders() string {
	return b.IpAddress
}

func (b *Bootstrap) GetPort() int {
	return b.Port
}

func (b *Bootstrap) GetFullAddress() string {
	return fmt.Sprintf("%s:%d", b.IpAddress, b.Port)
}

func (b *Bootstrap) GetType() string {
	return "Bootstrap"
}

func (b *Bootstrap) GetId() int {
	return -1
}

type Worker struct {
	Id          int                `json:"nodeId"`
	IpAddress   string             `json:"ipAddress"`
	Port        int                `json:"port"`
	prev        int                `json:"-"`
	next        int                `json:"-"`
	JobId       int                `json:"-"`
	FractalId   int                `json:"-"`
	Connections []int              `json:"-"`
	History     []structures.Point `json:"-"`
}

func (w *Worker) GetAdders() string {
	return w.IpAddress
}

func (w *Worker) GetPort() int {
	return w.Port
}

func (w *Worker) GetFullAddress() string {
	return fmt.Sprintf("%s:%d", w.IpAddress, w.Port)
}

func (w *Worker) GetType() string {
	return "Worker"
}

func (w *Worker) GetId() int {
	return w.Id
}
