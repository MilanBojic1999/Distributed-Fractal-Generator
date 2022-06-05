package modulemath

import (
	"strconv"
	"strings"
)

type ModMath struct {
	N int32 `json:"N"`
}

func reverse(str string) (result string) {
	for _, v := range str {
		result = string(v) + result
	}
	return
}

func (mm *ModMath) IntToMod(input int32) string {
	var sb strings.Builder
	for ; input > 0; input /= mm.N {
		sb.WriteString(strconv.Itoa(int(input % mm.N)))
	}

	return reverse(sb.String())
}

func (mm *ModMath) NextOne(input string) string {
	array := []rune(input)
	overflow := true
	outArray := make([]rune, 0)
	i := len(array) - 1
	for ; i >= 0 && overflow; i-- {
		digit := int(array[i] - '0')
		digit++
		if digit == int(mm.N) {
			digit = 0
		} else {
			overflow = false
		}
		outArray = append([]rune{rune('0' + digit)}, outArray...)
	}
	if overflow {
		if outArray[0] == '0' {
			outArray = append([]rune{'1'}, outArray...)
		}
	} else if i >= 0 {
		outArray = append(array[:i+1], outArray...)
	}

	return string(outArray)
}
