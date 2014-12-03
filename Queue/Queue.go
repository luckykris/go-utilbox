package Queue
import (
	"container/list"
	"errors"
	"github.com/luckykris/go-utilbox/Lock"
	"time"
)
//length of the queue

type Queue struct {
	list *list.List
	full  bool
	capacity int
	empty bool
	pushkey *Lock.KEY 
	popkey *Lock.KEY 
}
//new Queue
func NewQueue(c int) *Queue{
	l:=list.New()
	pushk:=Lock.New()
	popk:=Lock.New()
	return &Queue{list:l,capacity:c,full:false,pushkey:pushk,popkey:popk,empty:true}
}
//Push a value into Queue
func (self *Queue)Push(value interface{})(error){
	defer self.pushkey.Release()
	self.pushkey.Get()
	if self.capacity==0 || self.capacity>self.list.Len(){
		self.list.PushBack(value)
		self.empty=false
		return nil
	}else{
		return errors.New("Queue is Full! it will ignore the new item to push in")
	}
}

//Pop a value from the Queue
func (self *Queue)Pop()interface{}{
	defer self.popkey.Release()
	self.popkey.Get()	
	e:=self.list.Front()
	if e!=nil {
		self.list.Remove(e)
		return e.Value
	}else{
		return e
	}
}
//Pop for Block method
func (self *Queue)PopBlock()interface{}{
	defer self.popkey.Release()
	self.popkey.Get()
	for {
		if !self.empty{
			break
		}
		time.Sleep(10)
	}
	e:=self.list.Front()
	if e!=nil {
		self.list.Remove(e)
		return e.Value
	}else{

		return e
	}
}


// return length of quere
func (self *Queue)Len()int{
	return self.list.Len()
}
//return the bool of whether queue is empty
func (self *Queue)Empty()bool{
	return self.list.Len()==0
}
