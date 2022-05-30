package bootstrap

import (
	chanfile "distributed/chainfile"
	"distributed/massage"
	"distributed/node"
	"encoding/json"
	"fmt"
	"net"
	"os"
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

func RunBootstrap(ipAddres string, port int, FILE_SEPARATOR string) {

	BootstrapNode = node.Bootstrap{IpAddress: ipAddres, Port: port, Workers: make([]node.NodeInfo, 10)}

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

	go listenOnPort(ListenChan)
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
	case massage.Hail:
		proccesHailMassage(msgStruct)
	case massage.Join:
		proccesJoinMassage(msgStruct)
	case massage.Leave:
		proccesLeaveMassage(msgStruct)
	}

}

func proccesHailMassage(msg massage.Massage) {

	// toSend = massage.MakeContactMassage(BootstrapNode)

}

func proccesJoinMassage(msg massage.Massage) {

}

func proccesLeaveMassage(msg massage.Massage) {

}
