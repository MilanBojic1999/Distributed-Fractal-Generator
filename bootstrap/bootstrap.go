package bootstrap

import (
	chanfile "distributed/chainfile"
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

var BootstrapTableMutex sync.Mutex

func RunBootstrap(ipAddres string, port int, FILE_SEPARATOR string) {

	BootstrapNode = node.Bootstrap{IpAddress: ipAddres, Port: port, Workers: make([]node.NodeInfo, 10)}

	EnterenceChannel = make(chan int, 1)
	EnterenceChannel <- 1

	LogFile, err := os.Create(fmt.Sprintf("files%soutput%sbootstrapLog.log", FILE_SEPARATOR, FILE_SEPARATOR))
	if err != nil {
		check(err, "LogFile")
	}

	ErrorFile, err := os.Create(fmt.Sprintf("files%serror%sbootstrapError.log", FILE_SEPARATOR, FILE_SEPARATOR))
	if err != nil {
		check(err, "LogFile")
	}

	LogFileChan = make(chan string)
	LogErrorChan = make(chan string)

	ListenChan := make(chan int32)

	WritenFile := chanfile.ChanFile{File: LogFile, InputChan: LogFileChan}
	ErrorWritenFile := chanfile.ChanFile{File: ErrorFile, InputChan: LogErrorChan}

	go ErrorWritenFile.WriteFileFromChan()
	go WritenFile.WriteFileFromChan()

	LogFileChan <- "Bootstrap is running"

	listenOnPort(ListenChan)
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

	LogFileChan <- "Finally Recived " + msgStruct.Log()

	switch msgStruct.MassageType {
	case massage.Hail:
		go proccesHailMassage(msgStruct)
	case massage.Join:
		go proccesJoinMassage(msgStruct)
	case massage.Leave:
		go proccesLeaveMassage(msgStruct)

	}

}

func proccesHailMassage(msg massage.Massage) {

	<-EnterenceChannel // ulazimo u kriticnu sekciju
	var toSend *massage.Massage
	if len(BootstrapNode.Workers) == 0 {
		toSend = massage.MakeContactMassage(BootstrapNode, node.NodeInfo{Id: -1, IpAddress: "", Port: 0})

	} else {
		toSend = massage.MakeContactMassage(BootstrapNode, BootstrapNode.Workers[len(BootstrapNode.Workers)-1])
	}
	sendMessage(BootstrapNode.GetNodeInfo(), &msg.OriginalSender, toSend)
	EnterenceChannel <- 1 // izlazimo iz kriticne sekcije
}

func proccesJoinMassage(msg massage.Massage) {
	BootstrapNode.Workers = append(BootstrapNode.Workers, msg.OriginalSender)
}

func proccesLeaveMassage(msg massage.Massage) {
	BootstrapTableMutex.Lock()
	defer BootstrapTableMutex.Unlock()

	nodeIndex := -1
	for ind, val := range BootstrapNode.Workers {
		if val.Id == msg.OriginalSender.Id {
			nodeIndex = ind
			break
		}
	}

	if nodeIndex == -1 {
		LogErrorChan <- fmt.Sprintf("Tried to remove node from the system: %v", msg.OriginalSender.String())
		return
	}

	copy(BootstrapNode.Workers[nodeIndex:], BootstrapNode.Workers[nodeIndex+1:])
	BootstrapNode.Workers = BootstrapNode.Workers[:len(BootstrapNode.Workers)-1]
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
