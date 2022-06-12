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
	Id          int                 `json:"Id"`
	IpAddress   string              `json:"ipAddress"`
	Port        int                 `json:"port"`
	Prev        int                 `json:"-"`
	Next        int                 `json:"-"`
	JobName     string              `json:"-"`
	FractalId   string              `json:"-"`
	Connections map[string]NodeInfo `json:"-"`
	History     []structures.Point  `json:"-"`
	SystemInfo  map[int]NodeInfo    `json:"-"`
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
	toReturn.JobName = w.JobName
	toReturn.FractalId = w.FractalId

	return toReturn
}

func (w *Worker) String() string {
	return fmt.Sprintf("%d >< %s:%d (%s:%s) NEXT: %d & PREV: %d", w.Id, w.IpAddress, w.Port, w.JobName, w.FractalId, w.Next, w.Prev)
}

type NodeInfo struct {
	Id        int    `json:"Id"`
	IpAddress string `json:"ipAddress"`
	Port      int    `json:"port"`
	JobName   string `json:"JobName"`
	FractalId string `json:"FractalId"`
}

func (w *NodeInfo) GetFullAddress() string {
	return fmt.Sprintf("%s:%d", w.IpAddress, w.Port)
}

func (w *NodeInfo) String() string {
	return fmt.Sprintf("%d >< %s:%d (%s:%s)", w.Id, w.IpAddress, w.Port, w.JobName, w.FractalId)
}
