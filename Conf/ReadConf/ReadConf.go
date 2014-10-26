package ReadConf
import (
    "github.com/luckykris/go-utilbox/Conf"
    "github.com/luckykris/go-utilbox/Env"
    "os"
    "fmt"
)

// Config Area , for example
// [abc]  -->this is config area
// a=1 -->this is config row
type CONFIG struct {
	AREA string
	CONF map[string]CONFIGROW
}
type CONFIGROW struct {
	TYPE string
	EXIT bool
	DEFAULT interface{}
}
// config object ,to init configuration file
var CF *conf.ConfigFile
func LoadConf(path string,allcf ...CONFIG){
	cf, err := conf.ReadConfigFile(path)
    if err!=nil{
        fmt.Println(err)
        os.Exit(-1)
    }else{
    	CF=cf
    	for ar:=0;ar<len(allcf);ar++{
    		_AreaConf(allcf[ar])	    		
    	}
    }
}
// read row conf
func _RowConf(an string,cn string,cr CONFIGROW){
	switch cr.TYPE {
	case "int":
		r,err:=CF.GetInt(an,cn)
		__RowConfErrorHandle(r,err,an,cn,cr)
	case "string":
		r,err:=CF.GetString(an,cn)
		__RowConfErrorHandle(r,err,an,cn,cr)
	}
}

// error handel of row conf
func __RowConfErrorHandle(r interface{},err error,an string,cn string,cr CONFIGROW){
	if err==nil{
      		Env.Global(an+"/"+cn,r)
   	}else{
   		if cr.EXIT==true{
   			fmt.Printf("configuration Error: %s->%s is not be specific ",an,cn)
   			os.Exit(-1)
   		}
   		Env.Global(an+"/"+cn,cr.DEFAULT)
   	}
}
// area config analysis
func _AreaConf(area CONFIG){
	for cn,cr:= range area.CONF{
		_RowConf(area.AREA,cn,cr)
	}
}
