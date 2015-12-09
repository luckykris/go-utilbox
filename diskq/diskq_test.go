package diskq

import (
	"fmt"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	a, b := New("diskq_test.txt", "", 5, 10)
	if b != nil {
		fmt.Println(b)
	}
	a.Put([]byte(`string`))
	a.Put([]byte(`1111`))
	a.Put([]byte(`2222`))
	//fd.Write(c)
	c, _ := a.Get()
	if string(c) == string([]byte(`string`)) {
		t.Logf("ok")
	}
}
