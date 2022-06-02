package worker

import (
	chanfile "distributed/chainfile"
	"distributed/job"
	"distributed/massage"
	"distributed/node"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

func check(e error, addition string) {
	if e != nil {
		// fmt.Println(e)
		LogErrorChan <- e.Error() + addition
	}
}

var LogFileChan chan string
var LogErrorChan chan string

var BootstrapNode node.Bootstrap

var EnterenceChannel chan int
var WorkerEnteredChannel chan int

var BootstrapTableMutex sync.Mutex

var allJobs []job.Job

var WorkerNode node.Worker

func RunWorker(ipAddres string, port int, bootstrapIpAddres string, bootstrapPort int, jobs []job.Job, FILE_SEPARATOR string) {

	BootstrapNode = node.Bootstrap{IpAddress: ipAddres, Port: port, Workers: make([]node.NodeInfo, 1)}

	WorkerNode = node.Worker{}
	WorkerNode.IpAddress = ipAddres
	WorkerNode.Port = port

	copy(allJobs, jobs)

	LogFile, err := os.Create(fmt.Sprintf("files%soutput%sbootstrapLog.log", FILE_SEPARATOR, FILE_SEPARATOR))
	if err != nil {
		check(err, "LogFile")
	}

	ErrorFile, err := os.Create(fmt.Sprintf("files%serror%sbootstrapError.log", FILE_SEPARATOR, FILE_SEPARATOR))
	if err != nil {
		check(err, "LogFile")
	}

	EnterenceChannel = make(chan int, 1)
	WorkerEnteredChannel = make(chan int, 1)
	EnterenceChannel <- 1

	LogFileChan = make(chan string)
	LogErrorChan = make(chan string)

	ListenChan := make(chan int32)

	WritenFile := chanfile.ChanFile{File: LogFile, InputChan: LogFileChan}
	ErrorWritenFile := chanfile.ChanFile{File: ErrorFile, InputChan: LogErrorChan}

	go ErrorWritenFile.WriteFileFromChan()
	go WritenFile.WriteFileFromChan()

	go listenOnPort(ListenChan)

	enterneceSystemMassage := massage.MakeHailMassage(WorkerNode, BootstrapNode)
	sendMessage(WorkerNode.GetNodeInfo(), BootstrapNode.GetNodeInfo(), enterneceSystemMassage)

	<-WorkerEnteredChannel // we wait to enter to system

}

func listenOnPort(listenChan chan int32) {
	laddr, err := net.ResolveTCPAddr("tcp", BootstrapNode.GetFullAddress())
	if err != nil {
		fmt.Println(err)
		check(err, "ResolveTCPAddr")
		return
	}
	ln, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		fmt.Println(err)
		check(err, "ResolveTCPAddr")
		return
	}
	ln.SetDeadline(time.Now().Add(10 * time.Second))

	for {
		select {
		case val := <-listenChan:
			fmt.Println(val)
			ln.Close()
			return
		default:
			inMsg, err := ln.Accept()
			if err != nil {
				if err, ok := err.(net.Error); ok {
					check(err, "(net.Error)")

					ln.SetDeadline(time.Now().Add(10 * time.Second))
				}
			} else {
				var msgStruct massage.Massage
				json.NewDecoder(inMsg).Decode(&msgStruct)
				processRecivedMassage(msgStruct)

				inMsg.Close()
			}
		}
		time.Sleep(time.Millisecond * 200)
	}
}

func processRecivedMassage(msgStruct massage.Massage) {
	switch msgStruct.MassageType {
	case massage.Contact:
		go proccesContactMassage(msgStruct)
	case massage.Welcome:
		go proccesWelcomeMassage(msgStruct)
	case massage.Entered:
		go proccesEnteredMassage(msgStruct)
	case massage.SystemKnock:
		go proccesSystemKnockMassage(msgStruct)
	}
}

func proccesContactMassage(msgStruct massage.Massage) {

	var ContactInfo node.NodeInfo

	json.Unmarshal([]byte(msgStruct.GetMassage()), &ContactInfo)
	if ContactInfo.Id == -1 {
		WorkerNode.Id = 0
		toSend := massage.MakeJoinMassage(WorkerNode, BootstrapNode)
		go sendMessage(WorkerNode.GetNodeInfo(), BootstrapNode.GetNodeInfo(), toSend)
		WorkerEnteredChannel <- 1
	} else {
		knockMassage := massage.MakeSystemKnockMassage(WorkerNode, ContactInfo)
		sendMessage(WorkerNode.GetNodeInfo(), &ContactInfo, knockMassage)
	}

}

func proccesWelcomeMassage(msgStruct massage.Massage) {

}

func proccesSystemKnockMassage(msgStruct massage.Massage) {

}

func proccesEnteredMassage(msgStruct massage.Massage) {

}

func sendMessage(sender, reciver *node.NodeInfo, msg *massage.Massage) bool {
	connOut, err := net.DialTimeout("tcp", reciver.GetFullAddress(), time.Duration(1)*time.Second)
	if err != nil {
		if _, ok := err.(net.Error); ok {
			// fmt.Println("Error received while connecting to ", reciver.NodeId)
			check(err, "sendMessage")
			return false
		}
	} else {
		json.NewEncoder(connOut).Encode(&msg)
		connOut.Close()
	}

	return true
}
