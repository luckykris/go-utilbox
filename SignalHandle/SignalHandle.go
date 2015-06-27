//version 1.0
//last modify:2015.06.27
package SignalHandle
import (
    "os"
    "os/signal"
    "syscall"
    "runtime"
)


func StartSignalHandle(signaltype string ,function func(),once bool){
	go SignalHandle(signaltype,function,once)
}

func SignalHandle(signaltype string ,function func(),once bool){
	runtime.Gosched()
	for {
		ch := make(chan os.Signal)
 		signal.Notify(ch, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2,syscall.SIGHUP)
 		sig := <-ch
 		v:=sig.String()
 		if v==signaltype{
 				function()
				if once{
					return
				}
 			}
		}
}
