package Env
var ENV=map[string]interface{}{}

// Assign a value to the variable
func Global(n string,v interface{}){
	ENV[n]=v
}
