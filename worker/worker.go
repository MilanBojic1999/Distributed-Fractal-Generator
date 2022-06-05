package worker

import (
	"bufio"
	chanfile "distributed/chainfile"
	"distributed/job"
	"distributed/message"
	"distributed/node"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
)

func check(e error, addition string) {
	if e != nil {
		// fmt.Println(e)
		LogErrorChan <- e.Error() + addition
	}
}

var LogFileChan chan string
var LogErrorChan chan string

var ListenPortListenChan chan int32
var CommandPortListenChan chan int32

var BootstrapNode node.Bootstrap

var EnterenceChannel chan int
var WorkerEnteredChannel chan int

var WorkerTableMutex sync.Mutex
var WorkerEnterenceMutex sync.Mutex
var ConnectionWaitGroup sync.WaitGroup

var allJobs map[string]job.Job

var WorkerNode node.Worker

func RunWorker(ipAddres string, port int, bootstrapIpAddres string, bootstrapPort int, jobs []job.Job, FILE_SEPARATOR string) {

	BootstrapNode = node.Bootstrap{IpAddress: bootstrapIpAddres, Port: bootstrapPort, Workers: make([]node.NodeInfo, 1)}

	WorkerNode = node.Worker{}
	WorkerNode.IpAddress = ipAddres
	WorkerNode.Port = port

	WorkerNode.SystemInfo = make(map[int]node.NodeInfo)

	for _, v := range jobs {
		if v, ok := allJobs[v.Name]; !ok {
			LogErrorChan <- fmt.Sprintf("Job already exist: %v", v)
			continue
		}
		allJobs[v.Name] = v
	}

	fmt.Printf("\nWut: %v\n", allJobs)

	LogFile, err := os.Create(fmt.Sprintf("files%soutput%sworker(%s_%d).log", FILE_SEPARATOR, FILE_SEPARATOR, ipAddres, port))
	if err != nil {
		check(err, "LogFile")
	}

	ErrorFile, err := os.Create(fmt.Sprintf("files%serror%sworker(%s_%d).log", FILE_SEPARATOR, FILE_SEPARATOR, ipAddres, port))
	if err != nil {
		check(err, "LogFile")
	}

	EnterenceChannel = make(chan int, 1)
	WorkerEnteredChannel = make(chan int, 1)
	EnterenceChannel <- 1

	LogFileChan = make(chan string, 15)
	LogErrorChan = make(chan string, 15)

	ListenPortListenChan = make(chan int32, 2)
	CommandPortListenChan = make(chan int32, 2)

	WritenFile := chanfile.ChanFile{File: LogFile, InputChan: LogFileChan}
	ErrorWritenFile := chanfile.ChanFile{File: ErrorFile, InputChan: LogErrorChan}

	LogFileChan <- fmt.Sprintf("%v", jobs)

	go ErrorWritenFile.WriteFileFromChan()
	go WritenFile.WriteFileFromChan()

	go listenOnPort(ListenPortListenChan)

	enterneceSystemMessage := message.MakeHailMessage(WorkerNode, BootstrapNode)
	sendMessage(WorkerNode.GetNodeInfo(), BootstrapNode.GetNodeInfo(), enterneceSystemMessage)

	<-WorkerEnteredChannel // we wait to enter to system

	LogFileChan <- "Worker is working"
	fmt.Println(WorkerNode.SystemInfo)

	if len(WorkerNode.SystemInfo) > 1 {
		makeInitConnections()
	}

	listenCommand(CommandPortListenChan)

	fmt.Println("JOH")
}

func listenOnPort(listenChan chan int32) {
	laddr, err := net.ResolveTCPAddr("tcp", WorkerNode.GetFullAddress())
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
	case message.Contact:
		go proccesContactMessage(msgStruct)
	case message.Welcome:
		go proccesWelcomeMessage(msgStruct)
	case message.Entered:
		go proccesEnteredMessage(msgStruct)
	case message.SystemKnock:
		go proccesSystemKnockMessage(msgStruct)
	case message.ConnectionRequest:
		go proccesConnectionRequest(msgStruct)
	case message.ConnectionResponse:
		go proccesConnectionResponse(msgStruct)
	case message.Purge:
		go proccesPurgeResponse(msgStruct)
	}
}

func proccesContactMessage(msgStruct message.Message) {

	var ContactInfo node.NodeInfo

	json.Unmarshal([]byte(msgStruct.GetMessage()), &ContactInfo)

	if ContactInfo.Id == -1 {
		WorkerNode.Id = 0
		toSend := message.MakeJoinMessage(*WorkerNode.GetNodeInfo(), *BootstrapNode.GetNodeInfo())
		LogFileChan <- "Entered system with id 0. I'm the first one"

		go sendMessage(WorkerNode.GetNodeInfo(), BootstrapNode.GetNodeInfo(), toSend)
		WorkerNode.SystemInfo[0] = *WorkerNode.GetNodeInfo()
		WorkerEnteredChannel <- 1
	} else {
		knockMessage := message.MakeSystemKnockMessage(*WorkerNode.GetNodeInfo(), ContactInfo)
		sendMessage(WorkerNode.GetNodeInfo(), &ContactInfo, knockMessage)
	}

}

func proccesWelcomeMessage(msgStruct message.Message) {

	var msgMap map[string]interface{}

	json.Unmarshal([]byte(msgStruct.GetMessage()), &msgMap)

	newNodeId, ok := msgMap["id"].(float64)
	if !ok {
		LogErrorChan <- fmt.Sprintf("¦1¦Wrong message recived: %s", msgStruct.GetMessage())
		return
	}
	WorkerNode.Id = int(newNodeId)
	SystemInfoRecived := msgMap["systemInfo"].(map[string]interface{})

	if !ok {
		LogErrorChan <- fmt.Sprintf("¦2¦Wrong message recived: %s", msgStruct.GetMessage())
		return
	}

	for kstr, v := range SystemInfoRecived {
		k, _ := strconv.Atoi(kstr)
		var tmpNI node.NodeInfo
		mapstructure.Decode(v, &tmpNI)
		fmt.Printf("LOLOL: %v %v\n", v, tmpNI.String())
		WorkerNode.SystemInfo[k] = tmpNI
	}
	WorkerNode.SystemInfo[WorkerNode.Id] = *WorkerNode.GetNodeInfo()

	LogFileChan <- fmt.Sprintf("Finnaly entered system with id %d ", WorkerNode.Id)

	LogFileChan <- fmt.Sprintf("System info: %v", WorkerNode.SystemInfo)

	toSend := message.MakeEnteredMessage(*WorkerNode.GetNodeInfo())
	go broadcastMessage(&WorkerNode, toSend)

	toSendBootstrap := message.MakeJoinMessage(*WorkerNode.GetNodeInfo(), *BootstrapNode.GetNodeInfo())
	go sendMessage(WorkerNode.GetNodeInfo(), BootstrapNode.GetNodeInfo(), toSendBootstrap)
	WorkerEnteredChannel <- 1
}

func proccesSystemKnockMessage(msgStruct message.Message) {

	WorkerEnterenceMutex.Lock()
	defer WorkerEnterenceMutex.Unlock()

	LogFileChan <- fmt.Sprintf("Node: %v knocked on this system. I'm contact.", msgStruct.OriginalSender)

	maxIndex := WorkerNode.Id
	for _, val := range WorkerNode.SystemInfo {
		if maxIndex < val.Id {
			maxIndex = val.Id
		}
	}
	if maxIndex != WorkerNode.Id {
		LogFileChan <- fmt.Sprintf("Node: %v knocked on this system,But Im not youngest in the system (Node %d)", msgStruct.OriginalSender, maxIndex)
		tmp := WorkerNode.SystemInfo[maxIndex]
		newMessage := msgStruct.MakeMeASender(&WorkerNode)
		sendMessage(&msgStruct.OriginalSender, &tmp, newMessage)
		return
	}

	toSand := message.MakeWelcomeMessage(*WorkerNode.GetNodeInfo(), msgStruct.OriginalSender, maxIndex+1, WorkerNode.SystemInfo)
	sendMessage(WorkerNode.GetNodeInfo(), &msgStruct.OriginalSender, toSand)
}

func proccesEnteredMessage(msgStruct message.Message) {

	var newNodeInfo node.NodeInfo
	json.Unmarshal([]byte(msgStruct.Message), &newNodeInfo)

	if val, ok := WorkerNode.SystemInfo[newNodeInfo.Id]; ok {
		LogErrorChan <- fmt.Sprintf("Tried to info system %v , but already have %v", newNodeInfo, val)
		return
	}

	WorkerNode.SystemInfo[newNodeInfo.Id] = newNodeInfo
	LogFileChan <- fmt.Sprintf("New node in the system: %v", newNodeInfo)
}

func proccesConnectionRequest(msgStruct message.Message) {

	direction := msgStruct.GetMessage()

	if strings.Compare(direction, string(message.Next)) == 0 {
		WorkerNode.Prev = msgStruct.OriginalSender.Id
	} else {
		WorkerNode.Next = msgStruct.OriginalSender.Id
	}

	toSend := message.MakeConnectionResponseMessage(*WorkerNode.GetNodeInfo(), msgStruct.GetSender(), true, message.ConnectionSmer(direction))
	sendMessage(WorkerNode.GetNodeInfo(), &msgStruct.OriginalSender, toSend)
}

func proccesConnectionResponse(msgStruct message.Message) {

	message_arr := strings.Split(msgStruct.GetMessage(), ":")

	accepted, _ := strconv.ParseBool(message_arr[0])
	direction := message_arr[1]

	if accepted {
		if strings.Compare(direction, string(message.Next)) == 0 {
			WorkerNode.Next = msgStruct.OriginalSender.Id
		} else {
			WorkerNode.Prev = msgStruct.OriginalSender.Id
		}
	}
	ConnectionWaitGroup.Done()
}

func proccesPurgeResponse(msgStruct message.Message) {

	CommandPortListenChan <- 1
	ListenPortListenChan <- 1
	LogFileChan <- "System purge"
}

func proccesClusterKnock(msgStruct message.Message) {

}

func makeInitConnections() {

	ConnectionWaitGroup.Add(2)
	toSendNext := message.MakeConnectionRequestMessage(*WorkerNode.GetNodeInfo(), WorkerNode.SystemInfo[0], message.Next)
	tmpNI := WorkerNode.SystemInfo[0]
	sendMessage(WorkerNode.GetNodeInfo(), &tmpNI, toSendNext)

	toSendPrev := message.MakeConnectionRequestMessage(*WorkerNode.GetNodeInfo(), WorkerNode.SystemInfo[WorkerNode.Id-1], message.Prev)
	tmpNI = WorkerNode.SystemInfo[WorkerNode.Id-1]
	sendMessage(WorkerNode.GetNodeInfo(), &tmpNI, toSendPrev)

	ConnectionWaitGroup.Wait()
}

func sendMessage(sender, reciver *node.NodeInfo, msg message.IMessage) bool {
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

func broadcastMessage(sender *node.Worker, msg message.IMessage) bool {
	result := true
	for _, val := range sender.SystemInfo {
		result = result && sendMessage(sender.GetNodeInfo(), &val, msg)
	}

	return result
}

func parseCommand(commandArg string) bool {

	command_arr := strings.SplitN(commandArg, " ", 2)
	command := command_arr[0]
	if strings.EqualFold(command, "quit") {
		fmt.Println("Quitting...")
		ListenPortListenChan <- 1
		time.Sleep(time.Second)
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
