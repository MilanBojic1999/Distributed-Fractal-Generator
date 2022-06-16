package structures

import (
	"reflect"
	"strconv"
)

type Stack []int

func (s *Stack) IsEmpty() bool {
	return len(*s) == 0
}

func (s *Stack) Push(val int) {
	*s = append(*s, val)
}

func (s *Stack) Pop() (int, bool) {
	if s.IsEmpty() {
		return 0, false
	} else {
		index := len(*s) - 1   // Get the index of the top most element.
		element := (*s)[index] // Index into the slice and obtain the element.
		*s = (*s)[:index]      // Remove it from the stack by slicing it off.
		return element, true
	}
}

type Queue []int

func (q *Queue) IsEmpty() bool {
	return len(*q) == 0
}

func (q *Queue) Enqueue(val int) {
	*q = append(*q, val)
}

func (q *Queue) Dequeue() (int, bool) {
	if q.IsEmpty() {
		return 0, false
	}
	elem := (*q)[0]
	*q = (*q)[1:]
	return elem, true
}

type Point struct {
	X int `json:"x"`
	Y int `json:"y"`
}

type MyFloat float64

func (f MyFloat) MarshalJSON() ([]byte, error) {
	if float64(f) == float64(int(f)) {
		return []byte(strconv.FormatFloat(float64(f), 'f', 1, 32)), nil
	}
	val := strconv.FormatFloat(float64(f), 'f', 3, 32)
	return []byte(val), nil
}

func (f MyFloat) AsFloat() float64 {
	ref := reflect.ValueOf(f)
	if ref.Kind() != reflect.Float64 {
		return 0.0
	}
	return float64(ref.Float())
}

func AbsoluteInt(v int) int {
	if v < 0 {
		return -v
	}

	return v
}
