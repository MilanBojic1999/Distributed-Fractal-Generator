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

var waitingChildrenArray []node.NodeInfo
var childrenWaiting int

var clusterMap map[string]node.NodeInfo

var WorkerNode node.Worker

var ModMath modulemath.ModMath

var ImageInfoWaitingGroup sync.WaitGroup
var ImageInfoChannel chan job.Job

var JobStatusWaitingGroup sync.WaitGroup
var JobStatusChannel chan job.JobStatus

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
	childrenWaiting = 0
	waitingChildrenArray = make([]node.NodeInfo, 0)

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

	ImageInfoChannel = make(chan job.Job, 100)
	JobStatusChannel = make(chan job.JobStatus, 100)

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
		case message.ClusterConnectionRequest:
			go proccesClusterConnectionRequest(msgStruct)
		case message.ClusterConnectionResponse:
			go proccesClusterConnectionResponse(msgStruct)
		case message.ShareJob:
			go proccesJobShare(msgStruct)
		case message.ImageInfoRequest:
			go proccesImageInfoRequest(msgStruct)
		case message.ImageInfo:
			go proccesImageInfoResponse(msgStruct)
		case message.StartJob:
			go proccesStartJob(msgStruct)
		case message.ApproachCluster:
			go proccesApproachCluster(msgStruct)
		case message.JobStatus:
			go proccesJobStatus(msgStruct)
		case message.JobStatusRequest:
			go proccesJobStatusRequest(msgStruct)
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

func proccesJobStatus(msgStruct message.Message) {

	var newJobStatus job.JobStatus
	json.Unmarshal([]byte(msgStruct.GetMessage()), &newJobStatus)

	JobStatusChannel <- newJobStatus
	JobStatusWaitingGroup.Done()
}

func proccesJobStatusRequest(msgStruct message.Message) {

	var jobStatus job.JobStatus

	if workingJob == nil {
		LogErrorChan <- "Asked for Job status but there is no job"
	} else {
		jobStatus = *workingJob.GetJobStatus(WorkerNode.FractalId)
	}

	toSend := message.MakeJobStatusMessage(*WorkerNode.GetNodeInfo(), msgStruct.GetSender(), jobStatus)
	nextNode := findNextNode(msgStruct.GetSender())

	sendMessage(WorkerNode.GetNodeInfo(), &nextNode, toSend)
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
	json.Unmarshal([]byte(msgStruct.Message), &input)

	fractalID := input["fractalID"]

	var ClusterInfoMap map[int]node.NodeInfo
	json.Unmarshal([]byte(input["ClusterInfo"]), &ClusterInfoMap)

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

func ReorganizeSystem() {
	for _, val := range WorkerNode.SystemInfo {
		// if val.Id == WorkerNode.Id {
		// 	continue
		// }

		toSend := message.MakeStopShareJobMessage(*WorkerNode.GetNodeInfo(), val)
		nextNode := findNextNode(val)
		sendMessage(WorkerNode.GetNodeInfo(), &nextNode, toSend)

		ImageInfoWaitingGroup.Add(1)
	}

	WorkingJobsMap := make(map[string]*job.Job)

	for _, jj := range allJobs {
		if jj.Working {
			WorkingJobsMap[jj.Name] = jj
		}
	}

	ImageInfoWaitingGroup.Wait()

	for j := 0; j < len(WorkerNode.SystemInfo); j++ {
		tmpJob := <-ImageInfoChannel
		if val, ok := WorkingJobsMap[tmpJob.Name]; !ok {
			// jobic := *allJobs[tmpJob.Name]
			// jobic.Working = true
			// // jobic.Points = make([]structures.Point, 0)
			// WorkingJobsMap[tmpJob.Name] = &jobic
			LogErrorChan <- "Unknown working job: " + val.Log()
		} else {
			val.Points = append(val.Points, tmpJob.Points...)
			WorkingJobsMap[val.Name] = val
		}
	}

	var workingJobs []job.Job
	for _, job := range WorkingJobsMap {
		workingJobs = append(workingJobs, *job)
	}

	noWorkingJobs := len(workingJobs)

	i := 0
	for ; i < noWorkingJobs; i++ {
		reciver := WorkerNode.SystemInfo[i]
		jobic := workingJobs[i]
		msg := message.MakeStartJobMessage(*WorkerNode.GetNodeInfo(), reciver, jobic.Name)
		nextNode := findNextNode(reciver)
		sendMessage(WorkerNode.GetNodeInfo(), &nextNode, msg)
	}

	jobInd := 0

	for ; i < len(WorkerNode.SystemInfo); i++ {
		reciver := WorkerNode.SystemInfo[i]
		contact := WorkerNode.SystemInfo[jobInd]

		msg := message.MakeApproachClusterMessage(*WorkerNode.GetNodeInfo(), reciver, contact)
		nextNode := findNextNode(reciver)
		sendMessage(WorkerNode.GetNodeInfo(), &nextNode, msg)

		jobInd = (jobInd + 1) % noWorkingJobs

	}

}

func scaleJob(jobInput *job.Job, scalePoint structures.Point, scale float64) *job.Job {
	newJob := new(job.Job)

	newJob.Name = jobInput.Name
	newJob.PointCount = jobInput.PointCount
	newJob.Height = jobInput.Height
	newJob.Width = jobInput.Width
	newJob.MainPoints = make([]structures.Point, newJob.PointCount)
	newJob.Points = make([]structures.Point, 0)

	for ind, point := range jobInput.MainPoints {
		newJob.MainPoints[ind] = nextPoint(point, scalePoint, scale)
	}

	for _, point := range jobInput.Points {
		newJob.Points = append(newJob.Points, nextPoint(point, scalePoint, scale))
	}

	return newJob
}

func splitWorkingJob() {

	JobProccesingPoisonChan <- 1

	scale := 1.0 / (float64(workingJob.PointCount - 1))

	for ind := 1; ind < workingJob.PointCount; ind++ {
		miniJob := scaleJob(workingJob, workingJob.MainPoints[ind], scale)
		toSend := message.MakeClusterJobSharingMessage(*WorkerNode.GetNodeInfo(), waitingChildrenArray[ind-1], *miniJob)

		sendMessage(WorkerNode.GetNodeInfo(), &waitingChildrenArray[ind-1], toSend)
	}

	workingJob = scaleJob(workingJob, workingJob.MainPoints[0], scale)
	waitingChildrenArray = make([]node.NodeInfo, 0)
	go startJob(workingJob)
}

func proccesEnteredCluster(msgStruct message.Message) {

	var node node.NodeInfo
	json.Unmarshal([]byte(msgStruct.Message), &node)

	if _, ok := clusterMap[node.FractalId]; ok {
		LogErrorChan <- fmt.Sprintf("Node with the same fractalId %s in Cluster", node.FractalId)
		return
	}

	if strings.HasPrefix(node.FractalId, WorkerNode.FractalId) {
		childrenWaiting++
		waitingChildrenArray = append(waitingChildrenArray, node)
		if childrenWaiting == workingJob.PointCount-1 {
			splitWorkingJob()
		}
	}

	WorkerNode.SystemInfo[node.Id] = node
	clusterMap[node.FractalId] = node
	allJobs[node.JobName].Working = true
}

func proccesClusterConnectionRequest(msgStruct message.Message) {
	if modulemath.EditDistance(msgStruct.GetSender().FractalId, WorkerNode.FractalId) != 1 {
		LogErrorChan <- fmt.Sprintf("Wrong Cluster Connection! Wrong Edit Distance %s", msgStruct.GetSender().FractalId)
		return
	}

	sender := msgStruct.GetSender()

	WorkerNode.Connections[sender.FractalId] = sender
	toSend := message.MakeClusterConnectionResponseMessage(sender, *WorkerNode.GetNodeInfo(), true)
	sendMessage(WorkerNode.GetNodeInfo(), &sender, toSend)
}

func proccesClusterConnectionResponse(msgStruct message.Message) {

	accept, _ := strconv.ParseBool(msgStruct.GetMessage())
	sender := msgStruct.GetSender()

	if !accept {
		LogErrorChan <- "Refused connection in cluster from " + sender.FractalId
	}

	WorkerNode.Connections[sender.FractalId] = sender
}

func proccesStartJob(msgStruct message.Message) {

	var tmpJob job.Job
	json.Unmarshal([]byte(msgStruct.Message), &tmpJob)

	if _, ok := allJobs[tmpJob.Name]; !ok {
		LogErrorChan <- fmt.Sprintf("Job %s doenst exist...", tmpJob.Log())
		allJobs[tmpJob.Name] = &tmpJob
	}

	workingJob = &tmpJob

	LogFileChan <- "Starting job: " + workingJob.Log()

	go startJob(workingJob)
}

func proccesApproachCluster(msgStruct message.Message) {

	var contact node.NodeInfo
	json.Unmarshal([]byte(msgStruct.Message), &contact)

	toSend := message.MakeClusterKnockMessage(*WorkerNode.GetNodeInfo(), contact)
	nextNode := findNextNode(contact)

	sendMessage(WorkerNode.GetNodeInfo(), &nextNode, toSend)

}

func proccesJobShare(msgStruct message.Message) {

	jobInput := new(job.Job)
	json.Unmarshal([]byte(msgStruct.Message), &jobInput)

	if _, ok := allJobs[jobInput.Name]; !ok {
		LogErrorChan <- "Job is given to compute, but doesn't exist " + jobInput.Name
	}

	workingJob = jobInput

	LogFileChan <- "Starting new job: " + workingJob.Log()

	allJobs[workingJob.Name].Working = true

	go startJob(workingJob)
}

func proccesImageInfoRequest(msgStruct message.Message) {

	if workingJob == nil {
		LogErrorChan <- "Asked for image info but dont having job"
	}

	toSend := message.MakeImageInfoMessage(*WorkerNode.GetNodeInfo(), msgStruct.OriginalSender, *workingJob)

	nextNode := findNextNode(msgStruct.GetSender())

	sendMessage(WorkerNode.GetNodeInfo(), &nextNode, toSend)

}

func proccesImageInfoResponse(msgStruct message.Message) {

	var tmpJob job.Job
	json.Unmarshal([]byte(msgStruct.Message), &tmpJob)

	ImageInfoChannel <- tmpJob
	ImageInfoWaitingGroup.Done()

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
		nextOne := findNextNode(val)
		result = result && sendMessage(sender.GetNodeInfo(), &nextOne, msg)
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
		toSend := message.MakeShareJobMessage(*WorkerNode.GetNodeInfo(), *job)
		broadcastMessage(&WorkerNode, toSend)
	}
	job.Working = true
	allJobs[job.Name] = job
	ReorganizeSystem()
	// go startJob(job)
}

func parseStopJob(name string) {
	LogFileChan <- "Stopping job: " + name
	job, ok := allJobs[name]
	if !ok {
		LogErrorChan <- "There is no job: " + name + ". Error no job to stop"

		// JobProccesingPoisonChan <- 1

		// toSend := message.MakeShareJobMessage(*WorkerNode.GetNodeInfo(), *job)
		// broadcastMessage(&WorkerNode, toSend)
		return
	}

	job.Working = false
	allJobs[job.Name] = job
	ReorganizeSystem()

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

func allJobsStatus() int {
	for _, node := range WorkerNode.SystemInfo {
		msg := message.MakeJobStatusRequestMessage(*WorkerNode.GetNodeInfo(), node)
		nextNode := findNextNode(node)

		sendMessage(WorkerNode.GetNodeInfo(), &nextNode, msg)
	}

	nodeWaiting := len(WorkerNode.SystemInfo)

	JobStatusWaitingGroup.Add(nodeWaiting)
	JobStatusWaitingGroup.Wait()

	jobStatusMap := make(map[string]job.JobStatus)

	for i := 0; i < nodeWaiting; i++ {
		tmpJobStatus := <-JobStatusChannel
		if val, ok := jobStatusMap[tmpJobStatus.Name]; !ok {
			jobStatusMap[tmpJobStatus.Name] = tmpJobStatus
		} else {
			val.WorkingNodes++
			val.PointsGenerated += tmpJobStatus.PointsGenerated
			for key := range tmpJobStatus.PointsPerNodes {
				val.PointsPerNodes[key] = tmpJobStatus.PointsPerNodes[key]
			}
		}
	}

	return nodeWaiting
}

func oneJobStatus(name string) int {

	nodeWaiting := 0

	for _, node := range WorkerNode.SystemInfo {
		if strings.EqualFold(name, node.JobName) {
			msg := message.MakeJobStatusRequestMessage(*WorkerNode.GetNodeInfo(), node)
			nextNode := findNextNode(node)

			sendMessage(WorkerNode.GetNodeInfo(), &nextNode, msg)
			nodeWaiting++
		}
	}

	JobStatusWaitingGroup.Add(nodeWaiting)
	JobStatusWaitingGroup.Wait()

	jobStatusMap := make(map[string]job.JobStatus)

	for i := 0; i < nodeWaiting; i++ {
		tmpJobStatus := <-JobStatusChannel
		if val, ok := jobStatusMap[tmpJobStatus.Name]; !ok {
			jobStatusMap[tmpJobStatus.Name] = tmpJobStatus
		} else {
			val.WorkingNodes++
			val.PointsGenerated += tmpJobStatus.PointsGenerated
			for key := range tmpJobStatus.PointsPerNodes {
				val.PointsPerNodes[key] = tmpJobStatus.PointsPerNodes[key]
			}
		}
	}

	return nodeWaiting
}

func oneNodeJobStatus(name, fractalID string) int {
	for _, node := range WorkerNode.SystemInfo {
		if strings.EqualFold(name, node.JobName) {
			msg := message.MakeJobStatusRequestMessage(*WorkerNode.GetNodeInfo(), node)
			nextNode := findNextNode(node)

			sendMessage(WorkerNode.GetNodeInfo(), &nextNode, msg)
			break
		}
	}

	JobStatusWaitingGroup.Add(1)
	JobStatusWaitingGroup.Wait()

	return 1
}

func parseStatusJob(args string) {
	LogFileChan <- "Status getting: " + args

	args_array := strings.SplitN(args, " ", 2)

	nodeWaiting := 0

	switch len(args_array) {
	case 0:
		nodeWaiting = allJobsStatus()
	case 1:
		nodeWaiting = oneJobStatus(args_array[0])
	case 2:
		nodeWaiting = oneNodeJobStatus(args_array[0], args_array[1])
	default:
		LogErrorChan <- "wrong number of arguments: " + args
	}

	jobStatusMap := make(map[string]job.JobStatus)

	for i := 0; i < nodeWaiting; i++ {
		tmpJobStatus := <-JobStatusChannel
		if val, ok := jobStatusMap[tmpJobStatus.Name]; !ok {
			jobStatusMap[tmpJobStatus.Name] = tmpJobStatus
		} else {
			val.WorkingNodes++
			val.PointsGenerated += tmpJobStatus.PointsGenerated
			for key := range tmpJobStatus.PointsPerNodes {
				val.PointsPerNodes[key] = tmpJobStatus.PointsPerNodes[key]
			}
		}
	}

	for _, jobstat := range jobStatusMap {
		fmt.Printf("Job %s with %d Working nodes has %d generated points\n", jobstat.Name, jobstat.WorkingNodes, jobstat.PointsGenerated)
		for key, val := range jobstat.PointsPerNodes {
			fmt.Printf("\t %s] %d\n", key, val)
		}
		fmt.Println("-----------------------------")
	}
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
	} else if strings.EqualFold(command, "stop") {
		parseStopJob(command_arr[1])
	} else if strings.EqualFold(command, "status") {
		parseStatusJob(command_arr[1])
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

func findNextNode(goal node.NodeInfo) node.NodeInfo {

	if WorkerNode.Prev == goal.Id || WorkerNode.Next == goal.Id {
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

	editDist := modulemath.EditDistance(WorkerNode.FractalId, goal.FractalId)

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
