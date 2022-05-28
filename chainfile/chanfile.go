package chanfile

import (
	"fmt"
	"os"
	"time"
)

type ChanFile struct {
	File      *os.File
	InputChan chan string
}

func (ch ChanFile) String() string {
	return fmt.Sprint(ch.File)
}

func (ch *ChanFile) WriteFileFromChan() {
	for {
		msg, ok := <-ch.InputChan
		if ok == false {
			break
		}
		time_string := time.Now().Format("01-02-2006 15:04:05")
		writen := fmt.Sprintf("[%v] %s\n", time_string, msg)
		ch.File.Write([]byte(writen))
	}
}
