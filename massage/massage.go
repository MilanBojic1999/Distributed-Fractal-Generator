package massage

import (
	"distributed/node"
	"encoding/json"
	"fmt"
	"sync/atomic"
)

type MassageType int32

const (
	Info                      MassageType = 0
	InfoBroadcast             MassageType = 1
	Hail                      MassageType = 2
	Contact                   MassageType = 3
	Welcome                   MassageType = 4
	Join                      MassageType = 5
	Leave                     MassageType = 6
	Entered                   MassageType = 7
	ConnectionRequest         MassageType = 8
	ConnectionResponse        MassageType = 9
	Quit                      MassageType = 10
	ClusterKnock              MassageType = 11
	EnterCluster              MassageType = 12
	ClusterConnectionRequest  MassageType = 13
	ClusterConnectionResponse MassageType = 14
	JobSharing                MassageType = 15
	ImageInfoRequest          MassageType = 16
	ImageInfo                 MassageType = 17
	SystemKnock               MassageType = 18
)

type MassageCounter struct {
	counter int32
}

func (cnt *MassageCounter) Inc() int32 {
	return atomic.AddInt32(&cnt.counter, 1)
}

func (cnt *MassageCounter) Dec() int32 {
	return atomic.AddInt32(&cnt.counter, -1)
}

func (cnt *MassageCounter) Get() int32 {
	return atomic.LoadInt32(&cnt.counter)
}

var MainCounter = MassageCounter{0}

type IMassage interface {
	String() string
	MakeMeASender(node node.INode) IMassage
	Effect(args interface{})
	Log() string
	GetSender() node.NodeInfo
	GetReciver() node.NodeInfo
	GetRoute() []int
	GetMassage() string
}

type Massage struct {
	MassageType    MassageType   `json:"massage_type"`
	OriginalSender node.NodeInfo `json:"sender"`
	Reciver        node.NodeInfo `json:"reciver"`
	Route          []int         `json:"route"`
	Massage        string        `json:"massage"`
	Id             int64         `json:"id"`
}

func (msg *Massage) String() string {
	return "Massage"
}

func (msg *Massage) Effect(args interface{}) {
}

func (msg *Massage) GetSender() node.NodeInfo {
	return msg.OriginalSender
}

func (msg *Massage) GetReciver() node.NodeInfo {
	return msg.Reciver
}

func (msg *Massage) GetRoute() []int {
	return msg.Route
}

func (msg *Massage) GetMassage() string {
	return msg.Massage
}

func (msg *Massage) Log() string {
	return fmt.Sprintf("%d¦%d¦%d¦%d¦%s", msg.OriginalSender.Id, msg.Reciver.Id, msg.Id, msg.MassageType, msg.Massage)
}

func (msg *Massage) MakeMeASender(node node.INode) IMassage {

	msgReturn := Massage{}
	msgReturn.Id = msg.Id
	msgReturn.Massage = msg.Massage
	msgReturn.MassageType = msg.MassageType

	msgReturn.OriginalSender = msg.OriginalSender
	msgReturn.Reciver = msg.Reciver

	msgReturn.Route = append(msg.Route, node.GetId())

	return &msgReturn

}

func MakeInfoMassage(sender, reciver node.INode, massage string) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = massage
	msgReturn.MassageType = Info

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeInfoBroadcastMassage(sender node.INode, massage string) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = massage
	msgReturn.MassageType = InfoBroadcast

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *new(node.NodeInfo)

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeHailMassage(sender node.Worker, reciver node.Bootstrap) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = "Hail"
	msgReturn.MassageType = Hail

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeContactMassage(sender node.Bootstrap, reciver, contact node.NodeInfo) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	contact_byte, _ := json.Marshal(contact)
	msgReturn.Massage = string(contact_byte)
	msgReturn.MassageType = Contact

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = reciver

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeWelcomeMassage(sender, reciver node.NodeInfo, nodeId int, systemInfo map[int]node.NodeInfo) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())

	msgMap := map[string]interface{}{"id": nodeId, "systemInfo": systemInfo}
	msgb, _ := json.Marshal(msgMap)
	msgReturn.Massage = string(msgb)
	msgReturn.MassageType = Welcome

	msgReturn.OriginalSender = sender
	msgReturn.Reciver = reciver

	msgReturn.Route = []int{sender.Id}

	return &msgReturn
}

func MakeSystemKnockMassage(sender node.Worker, reciver node.NodeInfo) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = "SystemKnock"
	msgReturn.MassageType = SystemKnock

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = reciver

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeJoinMassage(sender node.Worker, reciver node.Bootstrap) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = fmt.Sprint(sender.GetId())
	msgReturn.MassageType = Join

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeLeaveMassage(sender node.Worker, reciver node.Bootstrap) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = fmt.Sprint(sender.GetId())
	msgReturn.MassageType = Leave

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeEnteredMassage(sender node.Worker) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())

	msgjson, _ := json.Marshal(sender.GetNodeInfo())

	msgReturn.Massage = string(msgjson)
	msgReturn.MassageType = Entered

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	reciver := new(node.NodeInfo)
	reciver.Id = -1
	msgReturn.Reciver = *reciver

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

type ConnectionSmer string

const (
	Next ConnectionSmer = "NEXT"
	Prev ConnectionSmer = "PREV"
)

func MakeConnectionRequestMassage(sender, reciver node.Worker, smer ConnectionSmer) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = string(smer)
	msgReturn.MassageType = ConnectionRequest

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeConnectionResponseMassage(sender, reciver node.Worker, accepted bool, smer ConnectionSmer) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = fmt.Sprintf("%t:%v", accepted, smer)
	msgReturn.MassageType = ConnectionResponse

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeQuitMassage(sender node.Worker) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = fmt.Sprint(sender.GetId())
	msgReturn.MassageType = Quit

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *new(node.NodeInfo)

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeClusterKnockMassage(sender, reciver node.Worker) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = "ClusterKnock"
	msgReturn.MassageType = ClusterKnock

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeClusterEnterMassage(sender, reciver node.Worker) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = "EnterCluster"
	msgReturn.MassageType = EnterCluster

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeClusterConnectionRequestMassage(sender, reciver node.Worker) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = "ClusterConnectionRequest"
	msgReturn.MassageType = ClusterConnectionRequest

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeClusterConnectionResponseMassage(sender, reciver node.Worker, accepted bool) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = fmt.Sprint(accepted)
	msgReturn.MassageType = ClusterConnectionResponse

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeClusterJobSharingMassage(sender, reciver node.Worker, jobInfo string) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = jobInfo
	msgReturn.MassageType = JobSharing

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeImageInfoRequestMassage(sender, reciver node.Worker) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	msgReturn.Massage = "ImageInfoRequest"
	msgReturn.MassageType = ImageInfoRequest

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}

func MakeImageInfoMassage(sender, reciver node.Worker, points [][]int) *Massage {
	msgReturn := Massage{}

	msgReturn.Id = int64(MainCounter.Inc())
	points_json, _ := json.Marshal(points)
	msgReturn.Massage = string(points_json)
	msgReturn.MassageType = ImageInfo

	msgReturn.OriginalSender = *sender.GetNodeInfo()
	msgReturn.Reciver = *reciver.GetNodeInfo()

	msgReturn.Route = []int{sender.GetId()}

	return &msgReturn
}
