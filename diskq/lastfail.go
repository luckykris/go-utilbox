package diskq

import (
	"fmt"
	"io/ioutil"
	"os"
)
//the function below is just for 'shepherd'.
func (self *DiskQueue) PersistLastFail(job interface{}) error {
	var err error
	lastFailFileName := fmt.Sprintf("%s.lf", self.abPath)
	if job == nil {
		err = os.Remove(lastFailFileName)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
	} else {
		lastFailFile, err := os.OpenFile(lastFailFileName, os.O_RDWR|os.O_CREATE, 0600)
		_, err = lastFailFile.WriteString(job.(string))
		if err != nil {
			lastFailFile.Close()
			return err
		}
	}
	return nil
}

func (self *DiskQueue) GetLastFail() (interface{}, error) {
	lastFailFileName := fmt.Sprintf("%s.lf", self.abPath)
	lastFailFile, err := os.OpenFile(lastFailFileName, os.O_RDONLY, 0600)
	if err != nil {
		lastFailFile.Close()
		if os.IsExist(err) {
			return nil, nil
		} else {
			return nil, err
		}
	}
	jobb, err := ioutil.ReadFile(lastFailFileName)
	if err != nil {
		return nil, err
	}
	return string(jobb), nil
}
