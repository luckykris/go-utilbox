package SignalHandle
import (
    "os"
    "os/signal"
    "syscall"
)


func StartSignalHandle(signaltype string ,function func()){
	go SignalHandle(signaltype,function)
}

func SignalHandle(signaltype string ,function func()){
	for {
		ch := make(chan os.Signal)
 		signal.Notify(ch, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2,syscall.SIGHUP)
 		sig := <-ch
 		v:=sig.String()
 		if v==signaltype{
 				function()
 			}
		}
}