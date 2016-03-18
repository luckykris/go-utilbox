package exit
import "sync"

var SAFE chan struct{} = make(chan struct{})
var ALL sync.WaitGroup

func Now() {
	close(SAFE)
}
