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
	GetNodeInfo() *NodeInfo
}

type Bootstrap struct {
	IpAddress string     `json:"ipAddress"`
	Port      int        `json:"port"`
	Workers   []NodeInfo `json:"-"`
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

func (b *Bootstrap) GetNodeInfo() *NodeInfo {
	toReturn := new(NodeInfo)
	toReturn.Id = b.GetId()
	toReturn.IpAddress = b.GetAdders()
	toReturn.Port = b.GetPort()

	return toReturn
}

type Worker struct {
	Id          int                `json:"nodeId"`
	IpAddress   string             `json:"ipAddress"`
	Port        int                `json:"port"`
	Prev        int                `json:"-"`
	Next        int                `json:"-"`
	JobId       int                `json:"-"`
	FractalId   int                `json:"-"`
	Connections []int              `json:"-"`
	History     []structures.Point `json:"-"`
	SystemInfo  map[int]NodeInfo   `json:"-"`
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

func (w *Worker) GetNodeInfo() *NodeInfo {
	toReturn := new(NodeInfo)
	toReturn.Id = w.GetId()
	toReturn.IpAddress = w.GetAdders()
	toReturn.Port = w.GetPort()

	return toReturn
}

type NodeInfo struct {
	Id        int    `json:"nodeId"`
	IpAddress string `json:"ipAddress"`
	Port      int    `json:"port"`
}

func (w *NodeInfo) GetFullAddress() string {
	return fmt.Sprintf("%s:%d", w.IpAddress, w.Port)
}

func (w *NodeInfo) String() string {
	return fmt.Sprintf("%d >< %s:%d", w.Id, w.IpAddress, w.Port)
}
