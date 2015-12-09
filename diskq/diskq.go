package diskq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type DiskQueue struct {
	name            string
	dataPath        string
	abPath          string
	size            int64
	maxBytesPerFile int64
	readPos         int64
	writePos        int64
	readFileNum     int64
	writeFileNum    int64
	length          int64
	readFile        *os.File
	writeFile       *os.File
	metaFile        *os.File
	reader          *bufio.Reader
	writeBuf        bytes.Buffer
	sync.Mutex
	lenLock    sync.RWMutex
	readBlock  bool
	writeBlock bool
	emptyChan  chan bool
	fullChan   chan bool
	exitChan   chan bool
	running    int64 //1:running 0:not running
}

func New(name, dataPath string, size, maxBytesPerFile int64) (*DiskQueue, error) {
	d := DiskQueue{
		name:            name,
		dataPath:        dataPath,
		size:            size,
		maxBytesPerFile: maxBytesPerFile,
		readPos:         0,
		writePos:        0,
		readFileNum:     0,
		writeFileNum:    0,
		length:          0,
		running:         1,
	}
	var err error
	d.emptyChan = make(chan bool, 1)
	d.fullChan = make(chan bool, 1)
	d.exitChan = make(chan bool)
	d.abPath = dataPath + name
	initFileName := fmt.Sprintf("%s.d.0", d.abPath)
	d.writeFile, err = os.OpenFile(initFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil { //wrtieFile open failed
		return &d, err
	}
	d.readFile, err = os.OpenFile(initFileName, os.O_RDONLY, 0600)
	if err != nil { //readFile open failed
		return &d, err
	}
	return &d, nil
}

func (self *DiskQueue) Length() int64 {
	return atomic.LoadInt64(&self.length)
}

func (self *DiskQueue) Get(blk bool) ([]byte, error) {
	var err error
	if atomic.LoadInt64(&self.running) < 1 {
		return []byte{}, fmt.Errorf("disk queue is closed.")
	}
	self.lenLock.RLock()
	if self.Length() < 1 { //when queue is empty ,block this
		self.lenLock.RUnlock()
		if blk {
			self.readBlock = true
			select {
			case <-self.exitChan:
				return []byte{}, fmt.Errorf("disk queue is closing.")
			case <-self.emptyChan:
			}
		} else {
			return []byte{}, fmt.Errorf("disk queue is empty.")
		}
	} else {
		self.readBlock = false
		self.lenLock.RUnlock()
	}
	defer self.Unlock()
	self.Lock()
	// start transaction
	tmp_length := self.Length()
	if self.readPos > self.maxBytesPerFile { //change read file when readPos is more than limit of a single data file.
		self.readFile.Close()
		self.readFileNum++
		self.readPos = 0
		readFileName := fmt.Sprintf("%s.d.%d", self.abPath, self.readFileNum)
		self.readFile, err = os.OpenFile(readFileName, os.O_RDONLY, 0600)
		if err != nil { //readFile open failed
			return []byte{}, err
		}
		self.ClearHitroy()
	}
	if self.readPos > 0 {
		_, err = self.readFile.Seek(self.readPos, 0)
		if err != nil {
			self.readFile.Close()
			return []byte{}, err
		}
	}
	var msgSize int32
	self.reader = bufio.NewReader(self.readFile)
	err = binary.Read(self.reader, binary.BigEndian, &msgSize)
	if err != nil {
		self.readFile.Close()
		return []byte{}, err
	}
	readBuf := make([]byte, msgSize)
	l, err := io.ReadFull(self.reader, readBuf)
	if err != nil {
		self.readFile.Close()
		return []byte{}, err
	}
	self.readPos = self.readPos + int64(l) + 4
	tmp_length -= 1
	err = self.PersistMetaData(tmp_length) //update metadata
	if err != nil {
		self.metaFile.Close()
		//rollback
		self.readPos = self.readPos - int64(l) - 4
		tmp_length += 1
		self.metaFile = nil
		return []byte{}, err
	}
	self.lenLock.Lock()
	atomic.StoreInt64(&self.length, int64(tmp_length))
	if self.writeBlock && tmp_length == self.size-1 {
		self.fullChan <- true
		//	fmt.Println("release full blk")
	}
	self.lenLock.Unlock()
	//finish transaction
	return readBuf, nil
}

func (self *DiskQueue) Put(b []byte, blk bool) error {
	if atomic.LoadInt64(&self.running) < 1 {
		return fmt.Errorf("disk queue is closed.")
	}
	self.lenLock.RLock()
	if self.Length() > self.size-1 { //here is a bug  ,when the code is len == size ,it don`t work out
		self.lenLock.RUnlock()
		if blk {
			self.writeBlock = true
			//fmt.Printf("i`m block i am %s\n", string(b))
			select {
			case <-self.exitChan:
				return fmt.Errorf("disk queue is closing.")
			case <-self.fullChan:
				//	fmt.Printf("i`m unblock i am %s\n", string(b))
			}
		} else {
			return fmt.Errorf("disk queue is full.")
		}
	} else {
		self.writeBlock = false
		self.lenLock.RUnlock()
	}
	defer self.Unlock()
	self.Lock()
	// start transaction
	tmp_length := self.Length()
	var err error
	if self.writePos > self.maxBytesPerFile { //change read file when readPos is more than limit of a single data file.
		self.writeFile.Close()
		self.writeFileNum++
		self.writePos = 0
		writeFileName := fmt.Sprintf("%s.d.%d", self.abPath, self.writeFileNum)
		self.writeFile, err = os.OpenFile(writeFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil { //readFile open failed
			return err
		}
	}
	self.writeBuf.Reset()
	err = binary.Write(&self.writeBuf, binary.BigEndian, int32(len(b)))
	if err != nil {
		return err
	}
	_, err = self.writeBuf.Write(b)
	if err != nil {
		return err
	}
	_, err = self.writeFile.Write(self.writeBuf.Bytes())
	if err != nil {
		self.writeFile.Close()
		self.writeFile = nil
		return err
	}
	self.writeFile.Sync()
	tmp_length += 1
	self.writePos = self.writePos + int64(len(b)) + 4
	err = self.PersistMetaData(tmp_length)
	if err != nil {
		self.metaFile.Close()
		//rollbcak diskqueue
		self.writePos = self.writePos - int64(len(b)) - 4
		tmp_length -= 1
		self.metaFile = nil
		return err
	}
	self.lenLock.Lock()
	atomic.StoreInt64(&self.length, int64(tmp_length))
	if self.readBlock && tmp_length == 1 {
		self.emptyChan <- true
	}
	self.lenLock.Unlock()
	//finish transaction
	return nil
}

func (self *DiskQueue) PersistMetaData(tmp_length int64) error {
	var err error
	tmpFileName := fmt.Sprintf("%s.md.tmp.%d", self.abPath, rand.Int())
	self.metaFile, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil { //wrtieFile open failed
		return err
	}
	readPos := strconv.Itoa(int(self.readPos))
	writePos := strconv.Itoa(int(self.writePos))
	readFileNum := strconv.Itoa(int(self.readFileNum))
	writeFileNum := strconv.Itoa(int(self.writeFileNum))
	length := strconv.Itoa(int(tmp_length))

	self.metaFile.Write([]byte(readPos + "\x03" + writePos + "\x03" + length + "\x03" + readFileNum + "\x03" + writeFileNum))
	self.metaFile.Close()
	return os.Rename(tmpFileName, self.abPath+".md")
}

func (self *DiskQueue) LoadQueue() error {
	var i int
	meta, err := ioutil.ReadFile(self.abPath + ".md")
	if err != nil { //metaFile open failed
		return err
	}
	ml := strings.Split(string(meta), "\x03")
	i, err = strconv.Atoi(ml[0]) // load read position
	if err != nil {
		return err
	}
	self.readPos = int64(i) // load write position
	i, err = strconv.Atoi(ml[1])
	if err != nil {
		return err
	}
	self.writePos = int64(i)
	self.lenLock.Lock()
	i, err = strconv.Atoi(ml[2]) // load length of queue
	if err != nil {
		return err
	}
	self.lenLock.Unlock()
	atomic.StoreInt64(&self.length, int64(i))
	i, err = strconv.Atoi(ml[3]) // load read file
	if err != nil {
		return err
	}
	self.readFileNum = int64(i)
	readFileName := fmt.Sprintf("%s.d.%d", self.abPath, self.readFileNum)
	self.readFile, err = os.OpenFile(readFileName, os.O_RDONLY, 0600)
	if err != nil { //readFile open failed
		return err
	}
	i, err = strconv.Atoi(ml[4]) // load write file
	if err != nil {
		return err
	}
	self.writeFileNum = int64(i)
	writeFileName := fmt.Sprintf("%s.d.%d", self.abPath, self.writeFileNum)
	self.writeFile, err = os.OpenFile(writeFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil { //wrtieFile open failed
		return err
	}
	_, err = self.writeFile.Seek(self.writePos, 0) //init write position
	if err != nil {
		self.writeFile.Close()
		return err
	}
	return nil
}

func (self *DiskQueue) Close() {
	atomic.AddInt64(&self.running, -1)
	close(self.exitChan)
	self.Lock()
	self.writeFile.Close()
	self.readFile.Close()
	self.Unlock()
	return
}
func (self *DiskQueue) ClearHitroy() error {
	fn := fmt.Sprintf("%s.d.%d", self.abPath, self.readFileNum-2)
	innerErr := os.Remove(fn)
	if innerErr != nil && !os.IsNotExist(innerErr) {
		log.Printf("ERROR: diskqueue(%s) failed to remove data file - %s\n", innerErr.Error())
		return innerErr
	}
	return nil
}

func (self *DiskQueue) SnapShot() ([][]byte, error) {
	var qls [][]byte
	defer self.Unlock()
	self.Lock()
	tmp_readFileNum := self.readFileNum
	tmp_readPos := self.readPos
	var err error
	var tmp_reader *bufio.Reader
	var tmp_readFile *os.File
	var tmp_readFileName string
	var msgSize int32
	var tmp_readBuf []byte
	var l int
	for {
		if tmp_readFileNum > self.writeFileNum || (tmp_readFileNum == self.writeFileNum && tmp_readPos >= self.writePos) {
			break
		}
		tmp_readFileName = fmt.Sprintf("%s.d.%d", self.abPath, tmp_readFileNum)
		tmp_readFile, err = os.OpenFile(tmp_readFileName, os.O_RDONLY, 0600)
		if err != nil { //readFile open failed
			return qls, err
		}
		if tmp_readPos > 0 {
			if tmp_readPos > self.maxBytesPerFile {
				tmp_readFileNum++
				continue
			}
			_, err = tmp_readFile.Seek(tmp_readPos, 0)
			if err != nil {
				tmp_readFile.Close()
				return qls, err
			}
		}
		tmp_reader = bufio.NewReader(tmp_readFile)
		for {
			err = binary.Read(tmp_reader, binary.BigEndian, &msgSize)
			tmp_readBuf = make([]byte, msgSize)
			l, err = io.ReadFull(tmp_reader, tmp_readBuf)
			if err != nil {
				if err.Error() == "EOF" {
					tmp_readFileNum++
					tmp_readFile.Close()
					tmp_readPos = 0
					break
				} else {
					return qls, err
				}
			}
			qls = append(qls, tmp_readBuf)
			tmp_readPos = tmp_readPos + int64(l) + 4
		}
	}
	return qls, nil
}
