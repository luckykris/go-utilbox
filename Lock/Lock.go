//version 0.2
package Lock
type KEY struct{
	key chan int
	}
// Type of Key ,contain a function to do when get key
func New()*KEY{
	ikey:=make(chan int ,1)
	ikey <- 0
	return &KEY{key:ikey}
}
//Create a NewLock
func (self *KEY)Get(){
	<- self.key
	return 
}
//Release a Lock
func (self *KEY)Release(){
	self.key <- 0
	return
}


