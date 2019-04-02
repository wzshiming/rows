package rows

import (
	"fmt"
	"go/ast"
	"reflect"
	"strings"
	"sync"
)

var sm = sync.Map{}

// colAdjust Adjust col
func colAdjust(tt reflect.Type, key []string, fn func(reflect.StructField) string) [][]string {
	pn := tt.PkgPath() + "." + tt.Name()
	v, b := sm.Load(pn)
	if b {
		if ss, ok := v.([][]string); ok && ss != nil && len(key) == len(ss) {
			return ss
		}
		fmt.Println("error rows.colAdjust")
	}

	m := map[string]int{}
	for i, v := range key {
		m[v] = i
	}

	rk := make([][]string, len(key))
	colAdjustMap(tt, key, nil, m, rk, fn)

	sm.Store(pn, rk)
	return rk
}

// colAdjustMap Adjust col map
func colAdjustMap(tt reflect.Type, key []string, prefix []string, m map[string]int, rk [][]string,
	ff func(reflect.StructField) string) bool {

	b := false
	nf := tt.NumField()
	for i := 0; i != nf; i++ {
		fi := tt.Field(i)
		tv := ff(fi)
		if tv == "" {
			continue
		}

		if fi.Anonymous && fi.Type.Kind() == reflect.Struct &&
			colAdjustMap(fi.Type, key, append(prefix, fi.Name), m, rk, ff) {
			continue
		}

		if !ast.IsExported(fi.Name) {
			continue
		}

		if k, ok := m[tv]; ok && len(rk[k]) == 0 {
			rk[k] = append(prefix, fi.Name)
			b = true
		}
	}

	return b
}

// MakeFieldName returns a function that gets the value of a field
func MakeFieldName(tag string) func(fn reflect.StructField) string {
	return func(fn reflect.StructField) string {
		b := fn.Tag.Get(tag)
		dd := strings.Split(b, ",")[0]
		if dd == "-" {
			return ""
		}
		if dd == "" {
			return strings.ToLower(fn.Name)
		}
		return dd
	}
}
