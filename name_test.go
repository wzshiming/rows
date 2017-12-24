package rows

import "testing"

var data1 = []string{
	"helloWorld",
	"HelloWorld",
	"hello_world",
	"HELLO_WORLD",

	"_helloWorld_",
	"_HelloWorld_",
	"_HELLO_WORLD_",
	"_hello_world_",

	"_HELLO____WORLD_",
	"_hello____world_",
	"_hello__World_",
	"_Hello__World_",
	"_HEllo__WORLD_",
}

func TestName(t *testing.T) {
	for _, v := range data1 {
		d := Hump2Snake(v)
		if d != "hello_world" {
			t.Error(v, d)
		}
	}
	for _, v := range data1 {
		d := Snake2Hump(v)
		if d != "HelloWorld" {
			t.Error(v, d)
		}
	}
}
