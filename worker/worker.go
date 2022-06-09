package worker

import (
	"bufio"
	chanfile "distributed/chainfile"
	"distributed/job"
	"distributed/message"
	"distributed/modulemath"
	"distributed/node"
	"distributed/structures"
	"encoding/json"
	"fmt"
	"math/rand"
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

func partOfSlice(route []int, nodeId int) bool {
	for _, v := range route {
		if v == nodeId {
			return true
		}
	}

	return false
}

const IMAGE_PATH = "files/images"

var LogFileChan chan string
var LogErrorChan chan string

var ListenPortListenChan chan int32
var CommandPortListenChan chan int32
var JobProccesingPoisonChan chan int32

var BootstrapNode node.Bootstrap

var EnterenceChannel chan int
var WorkerEnteredChannel chan int

var WorkerTableMutex sync.Mutex
var WorkerEnterenceMutex sync.Mutex
var ConnectionWaitGroup sync.WaitGroup

var allJobs map[string]*job.Job

var workingJob *job.Job
var workingJobsInSystem int

var clusterMap map[string]node.NodeInfo

var WorkerNode node.Worker

var ModMath modulemath.ModMath

func RunWorker(ipAddres string, port int, bootstrapIpAddres string, bootstrapPort int, jobs []job.Job, FILE_SEPARATOR string) {

	LogFileChan = make(chan string, 15)
	LogErrorChan = make(chan string, 15)

	BootstrapNode = node.Bootstrap{IpAddress: bootstrapIpAddres, Port: bootstrapPort, Workers: make([]node.NodeInfo, 1)}

	WorkerNode = node.Worker{}
	WorkerNode.IpAddress = ipAddres
	WorkerNode.Port = port

	WorkerNode.SystemInfo = make(map[int]node.NodeInfo)
	fmt.Printf("\nWut: %v\n", jobs)

	allJobs = make(map[string]*job.Job)
	workingJob = nil
	workingJobsInSystem = 0

	for _, v := range jobs {
		fmt.Printf("Job: %v\n", v)

		if _, ok := allJobs[v.Name]; ok {
			LogErrorChan <- fmt.Sprintf("Job already exist: %v", v)
			fmt.Printf("Job already exist: %v\n", v)
			continue
		}
		vv := v
		allJobs[vv.Name] = &vv
	}

	fmt.Printf("\nWut2: %v\n", allJobs)

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

	ListenPortListenChan = make(chan int32, 2)
	CommandPortListenChan = make(chan int32, 2)
	JobProccesingPoisonChan = make(chan int32, 2)

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
	if msgStruct.GetReciver().Id == WorkerNode.GetId() {
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
		case message.JobSharing:
			go proccesJobSharing(msgStruct)
		case message.ClusterKnock:
			go proccesClusterKnock(msgStruct)
		case message.ClusterWelcome:
			go proccesClusterWelcome(msgStruct)
		case message.EnteredCluster:
			go proccesEnteredCluster(msgStruct)
		}
	} else {
		if partOfSlice(msgStruct.Route, WorkerNode.Id) {
			LogFileChan <- fmt.Sprintf("Recived Again not Rebroadcasting %s  ~~~ ROUTE: %v", msgStruct.Log(), msgStruct.Route)
			return
		}
		broadcastnext := false
		switch msgStruct.MessageType {
		case message.Entered:
			go proccesEnteredMessage(msgStruct)
			broadcastnext = true
		case message.Purge:
			go proccesPurgeResponse(msgStruct)
			broadcastnext = true

		case message.ShareJob:
			go proccesEnteredMessage(msgStruct)
			broadcastnext = true

		case message.Quit:
			go proccesEnteredMessage(msgStruct)
			broadcastnext = true

		}
		if broadcastnext {
			newMsg := msgStruct.MakeMeASender(&WorkerNode)
			LogFileChan <- fmt.Sprintf("Recived but ain't for me: %s \\ Broadcasting", msgStruct.Log())
			broadcastMessage(&WorkerNode, newMsg)
		} else {
			newMsg := msgStruct.MakeMeASender(&WorkerNode)
			nextNode := findNextNode(newMsg.GetReciver())
			sendMessage(WorkerNode.GetNodeInfo(), &nextNode, newMsg)
		}

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

func proccesJobSharing(msgStruct message.Message) {

	var newJob job.Job
	json.Unmarshal([]byte(msgStruct.GetMessage()), &newJob)

	if _, ok := allJobs[newJob.Name]; ok {
		LogErrorChan <- "New job already exist: " + newJob.Name
		return
	}

	allJobs[newJob.Name] = &newJob
}

func proccesClusterKnock(msgStruct message.Message) {

	lastFractalID := WorkerNode.FractalId
	clusterInfo := make(map[int]node.NodeInfo)
	for ind, val := range WorkerNode.SystemInfo {
		if val.JobName == WorkerNode.JobName {
			if ModMath.CompareTwoNumbs(lastFractalID, val.FractalId) < 0 {
				lastFractalID = val.FractalId
			}
			clusterInfo[ind] = val
		}
	}

	nextOne := ModMath.NextOne(lastFractalID)

	toSend := message.MakeClusterWelcomeMessage(*WorkerNode.GetNodeInfo(), msgStruct.GetSender(), nextOne, clusterInfo)
	sendMessage(WorkerNode.GetNodeInfo(), &msgStruct.OriginalSender, toSend)
}

func proccesClusterWelcome(msgStruct message.Message) {

	var input map[string]string
	json.Unmarshal([]byte(msgStruct.Message), input)

	fractalID := input["fractalID"]

	var ClusterInfoMap map[int]node.NodeInfo
	json.Unmarshal([]byte(input["ClusterInfo"]), ClusterInfoMap)

	for _, val := range ClusterInfoMap {
		WorkerNode.SystemInfo[val.Id] = val
		clusterMap[val.FractalId] = val
	}

	WorkerNode.FractalId = fractalID

	for _, val := range WorkerNode.SystemInfo {
		toSend := message.MakeClusterEnteredMessage(*WorkerNode.GetNodeInfo(), val, *WorkerNode.GetNodeInfo())
		nextOne := findNextNode(val)

		sendMessage(WorkerNode.GetNodeInfo(), &nextOne, toSend)
	}
}

func proccesEnteredCluster(msgStruct message.Message) {

	var node node.NodeInfo
	json.Unmarshal([]byte(msgStruct.Message), node)

	if _, ok := clusterMap[node.FractalId]; ok {
		LogErrorChan <- fmt.Sprintf("Node with the same fractalId %s in Cluster", node.FractalId)
		return
	}

	WorkerNode.SystemInfo[node.Id] = node
	clusterMap[node.FractalId] = node
	allJobs[node.JobName].Working = true
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

func nextPoint(start, end structures.Point, ratio float64) structures.Point {
	new_x := (float64(start.X)*ratio + (1-ratio)*float64(end.X))
	new_y := (float64(start.Y)*ratio + (1-ratio)*float64(end.Y))
	// LogFileChan <- fmt.Sprintf("Float Points: %.2f %.2f (%.3f)", new_x, new_y, ratio)
	return structures.Point{X: int(new_x), Y: int(new_y)}
}

func startJob(job *job.Job) {
	point := job.MainPoints[0]
	for {
		select {
		case <-JobProccesingPoisonChan:
			LogFileChan <- "Ending job:" + job.Name
			return
		default:
			indPoint := rand.Intn(job.PointCount)
			point = nextPoint(point, job.MainPoints[indPoint], job.Ration)
			// LogFileChan <- fmt.Sprintf("New point: %v to Main point: %v", point, job.MainPoints[indPoint])
			job.Points = append(job.Points, point)
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func AskForNewJob(name string) *job.Job {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Number of points	:> ")
	text, _ := reader.ReadString('\n')
	text = strings.Replace(text, "\n", "", -1)

	pointCount, err := strconv.Atoi(text)
	if err != nil {
		check(err, "PointCount")
	}
	fmt.Println(pointCount, " ", text)

	fmt.Print("Ratio	:> ")
	text, _ = reader.ReadString('\n')
	text = strings.Replace(text, "\n", "", -1)

	ration, err := strconv.ParseFloat(text, 32)
	if err != nil {
		check(err, "Ratio")
	}

	fmt.Print("Height	:> ")
	text, _ = reader.ReadString('\n')
	text = strings.Replace(text, "\n", "", -1)

	height, err := strconv.Atoi(text)
	if err != nil {
		check(err, "Height")
	}

	fmt.Print("Width	:> ")
	text, _ = reader.ReadString('\n')
	text = strings.Replace(text, "\n", "", -1)

	width, err := strconv.Atoi(text)
	if err != nil {
		check(err, "Width")
	}

	points := make([]structures.Point, pointCount)
	for i := 0; i < pointCount; i++ {
		fmt.Printf("Point %d	:> ", i)
		text, _ = reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		text_arr := strings.SplitN(text, " ", 2)
		xstr, ystr := text_arr[0], text_arr[1]
		x, _ := strconv.Atoi(xstr)
		y, _ := strconv.Atoi(ystr)
		pp := structures.Point{X: x, Y: y}
		points[i] = pp
	}

	newJob := new(job.Job)
	newJob.Name = name
	newJob.PointCount = pointCount
	newJob.Height = height
	newJob.Width = width
	newJob.Ration = float64(ration)
	newJob.MainPoints = points
	newJob.Points = make([]structures.Point, 1)

	return newJob
}

func parseStartJob(name string) {
	LogFileChan <- "Starting job: " + name
	job, ok := allJobs[name]
	if !ok {
		LogFileChan <- "There is no job: " + name + ". Creating new job"
		job = AskForNewJob(name)
		allJobs[name] = job
		toSend := message.MakeShereJobMessage(*WorkerNode.GetNodeInfo(), *job)
		broadcastMessage(&WorkerNode, toSend)
	}
	go startJob(job)
}

func parseResultJob(args string) {
	LogFileChan <- "Result getting: " + args
	args_array := strings.SplitN(args, " ", 2)
	var name, fractalID string
	name = args_array[0]
	fractalID = ""
	if len(args_array) == 2 {
		fractalID = args_array[1]
		LogFileChan <- "KOJO " + fractalID
	}

	job, ok := allJobs[name]
	if !ok {
		LogErrorChan <- "There is no job: " + name
		return
	}
	job.MakeImage(IMAGE_PATH)
}

func parseCommand(commandArg string) bool {

	command_arr := strings.SplitN(commandArg, " ", 2)
	command := command_arr[0]
	if strings.EqualFold(command, "quit") {
		fmt.Println("Quitting...")
		ListenPortListenChan <- 1
		time.Sleep(time.Second)
		return false
	} else if strings.EqualFold(command, "start") {
		parseStartJob(command_arr[1])
	} else if strings.EqualFold(command, "result") {
		parseResultJob(command_arr[1])
	} else {
		fmt.Printf("Unknown command: %s\n", command)
	}
	return true

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

func editDistance(str1, str2 string) int {
	arr1 := []rune(str1)
	arr2 := []rune(str2)
	lenDiff := len(arr1) - len(arr2)
	if lenDiff != 0 {
		for i := 0; i < lenDiff; i++ {
			arr1 = append(arr1, '0')
		}
		for i := 0; i < (-lenDiff); i++ {
			arr2 = append(arr2, '0')
		}
	}

	dist := 0
	for i := 0; i < len(arr1); i++ {
		if arr1[i] != arr2[i] {
			dist++
		}
	}

	return dist
}

func findNextNode(goal node.NodeInfo) node.NodeInfo {

	if WorkerNode.Prev == goal.Port || WorkerNode.Next == goal.Port {
		return goal
	}

	if _, ok := WorkerNode.Connections[goal.FractalId]; ok {
		return goal
	}

	var v, u, nextNode node.NodeInfo
	if WorkerNode.Id > goal.Id {
		v, u = *WorkerNode.GetNodeInfo(), goal
	} else {
		v, u = goal, *WorkerNode.GetNodeInfo()
	}

	dist1 := v.Id - u.Id
	dist2 := len(WorkerNode.SystemInfo) - u.Id + v.Id
	var minDist int
	if dist1 > dist2 {
		nextNode = WorkerNode.SystemInfo[WorkerNode.Prev]
		minDist = dist2
	} else {
		nextNode = WorkerNode.SystemInfo[WorkerNode.Next]
		minDist = dist1
	}

	editDist := editDistance(WorkerNode.FractalId, goal.FractalId)

	if minDist > editDist {
		myArr := []rune(WorkerNode.FractalId)
		goalArr := []rune(goal.FractalId)

		sze := len(myArr)
		if sze > len(goalArr) {
			sze = len(goalArr)
		}

		for i := 0; i < sze; i++ {
			var tmpArr []rune
			copy(tmpArr, myArr)
			tmpArr[i] = goalArr[i]
			if v, ok := WorkerNode.Connections[string(tmpArr)]; ok {
				nextNode = v
				break
			}
		}
	}

	return nextNode
}
