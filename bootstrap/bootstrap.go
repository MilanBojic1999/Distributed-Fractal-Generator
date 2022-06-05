package bootstrap

import (
	"bufio"
	chanfile "distributed/chainfile"
	"distributed/message"
	"distributed/node"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
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

var ListenPortListenChan chan int32
var CommandPortListenChan chan int32

func RunBootstrap(ipAddres string, port int, FILE_SEPARATOR string) {

	BootstrapNode = node.Bootstrap{IpAddress: ipAddres, Port: port, Workers: make([]node.NodeInfo, 0, 10)}

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

	ListenPortListenChan = make(chan int32)
	CommandPortListenChan = make(chan int32)

	WritenFile := chanfile.ChanFile{File: LogFile, InputChan: LogFileChan}
	ErrorWritenFile := chanfile.ChanFile{File: ErrorFile, InputChan: LogErrorChan}

	go ErrorWritenFile.WriteFileFromChan()
	go WritenFile.WriteFileFromChan()

	LogFileChan <- "Bootstrap is running"

	go listenOnPort(ListenPortListenChan)

	listenCommand(CommandPortListenChan)
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
				var msgStruct message.Message
				json.NewDecoder(inMsg).Decode(&msgStruct)
				processRecivedMessage(msgStruct)

				inMsg.Close()
			}
		}
		time.Sleep(time.Millisecond * 200)
	}
}

func processRecivedMessage(msgStruct message.Message) {

	LogFileChan <- "Finally Recived " + msgStruct.Log()

	switch msgStruct.MessageType {
	case message.Hail:
		go proccesHailMessage(msgStruct)
	case message.Join:
		go proccesJoinMessage(msgStruct)
	case message.Leave:
		go proccesLeaveMessage(msgStruct)

	}

}

func proccesHailMessage(msg message.Message) {

	<-EnterenceChannel // ulazimo u kriticnu sekciju
	var toSend *message.Message
	fmt.Println(len(BootstrapNode.Workers))
	if len(BootstrapNode.Workers) == 0 {
		toSend = message.MakeContactMessage(*BootstrapNode.GetNodeInfo(), msg.GetSender(), node.NodeInfo{Id: -1, IpAddress: "rafhost", Port: -10})
	} else {
		toSend = message.MakeContactMessage(*BootstrapNode.GetNodeInfo(), msg.GetSender(), BootstrapNode.Workers[len(BootstrapNode.Workers)-1])
	}
	fmt.Println(toSend.Message)
	sendMessage(BootstrapNode.GetNodeInfo(), &msg.OriginalSender, toSend)
	EnterenceChannel <- 1 // izlazimo iz kriticne sekcije
}

func proccesJoinMessage(msg message.Message) {
	BootstrapNode.Workers = append(BootstrapNode.Workers, msg.OriginalSender)
}

func proccesLeaveMessage(msg message.Message) {
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

func sendMessage(sender, reciver *node.NodeInfo, msg *message.Message) bool {
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

func systemBroadcastMessage(msg *message.Message) {
	for _, v := range BootstrapNode.Workers {
		sendMessage(BootstrapNode.GetNodeInfo(), &v, msg)
	}
}

func parseCommand(commandArg string) bool {
	if len(commandArg) == 0 {
		return true
	}

	command_arr := strings.SplitN(commandArg, " ", 2)
	command := command_arr[0]
	if strings.EqualFold(command, "quit") {
		fmt.Println("Quitting...")
		ListenPortListenChan <- 1
		time.Sleep(time.Second)
		return false
	} else if strings.EqualFold(command, "purge") {
		toSend := message.MakePurgeMessage(*BootstrapNode.GetNodeInfo())
		go systemBroadcastMessage(toSend)
		ListenPortListenChan <- 1
		return false
	} else {
		fmt.Printf("Unknown command: %s\n", command)
		return true
	}
}

func listenCommand(listenChan chan int32) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Simple Shell")
	fmt.Println("---------------------")

	for {
		select {
		case <-listenChan:
			return
		default:
			fmt.Print(":> ")
			text, _ := reader.ReadString('\n')

			text = strings.Replace(text, "\n", "", -1)

			if !parseCommand(text) {
				return
			}

		}
	}
}
