package massage

import "sync/atomic"

type MassageType int32

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

type IMassage interface {
	String() string
	MakeMeASender() IMassage
	Effect(args interface{})
	Log() string
	GetSender() int
	GetReciver() int
	GetRoute() []int
	GetMassage() string
}
