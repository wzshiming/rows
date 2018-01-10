package rows

import (
	"fmt"
	"testing"
	"time"
)

type Hw struct {
	Hello2 string    `sql:"hello"`
	TT     time.Time `sql:"ti"`
	T      int
	Bool   bool
}

var (
	key  = []string{"hello", "ti", "t", "bool"}
	data = [][][]byte{
		{[]byte("world sdf"), []byte("2017-05-10 14:19:56"), []byte("12"), []byte("true")},
		{[]byte("world2"), []byte("2017-07-10T14:19:56+07:00"), []byte("3"), []byte("True")},
		{[]byte("world1232"), []byte("2017-07-12 14:19:56"), []byte("34"), []byte("false")},
	}
)

var fn = MakeFieldName("sql")

func TestRows(t *testing.T) {
	d0 := []Hw{}
	_, err := DataScanBytes(key, data, &d0, fn)
	if err != nil {
		t.Fatal(err)
		return
	}

	var d1 *[]*Hw
	_, err = DataScanBytes(key, data, &d1, fn)
	if err != nil {
		t.Fatal(err)
		return
	}

	if len(d0) != len(*d1) {
		t.Fatal("len(d0) != len(d1)")
		return
	}
	for k, v := range d0 {
		v1 := *(*d1)[k]
		if fmt.Sprint(v1) != fmt.Sprint(v) {
			t.Fatal("v1 != v")
			return
		}
	}

	d2 := []*Hw{}
	_, err = DataScanBytes(key, data, &d2, fn)
	if err != nil {
		t.Fatal(err)
		return
	}

	if len(d0) != len(d2) {
		t.Fatal("len(d0) != len(d2)")
		return
	}
	for k, v := range d0 {
		v2 := *d2[k]
		if fmt.Sprint(v2) != fmt.Sprint(v) {
			t.Fatal("v2 != v")
			return
		}
	}

	var d3 *Hw
	_, err = DataScanBytes(key, data, &d3, fn)
	if err != nil {
		t.Fatal(err)
		return
	}

	if fmt.Sprint(*d3) != fmt.Sprint(d0[0]) {
		t.Fatal("*d3 != d0[0]")
		return
	}

	var d4 Hw
	_, err = DataScanBytes(key, data, &d4, fn)
	if err != nil {
		t.Fatal(err)
		return
	}

	if fmt.Sprint(d4) != fmt.Sprint(d0[0]) {
		t.Fatal("d4 != d0[0]")
		return
	}

	return
}
