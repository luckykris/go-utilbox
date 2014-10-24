package Lock
//version 0.1
type KEY struct{
	key chan int
	function func()interface{} 
	}
// Type of Key ,contain a function to do when get key
func NewLock(ifunc func()interface{})*KEY{
	ikey:=make(chan int ,1)
	ikey <- 0
	return &KEY{key:ikey,function:ifunc}
}
//Create a NewLock
func (self *KEY)GetKey()interface{}{
	<- self.key
	tmp:=self.function()
    self.key <- 0
    return tmp
}
//Try to get a key,when other one has got it, it will block your program,until someone release the key, and return a value that your function return which youu define in function of 'NewLock'
