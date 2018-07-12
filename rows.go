package rows

import (
	"fmt"
	"reflect"
)

// Rows Handle multiline interfaces
type Rows interface {
	Next() bool
	Columns() ([]string, error)
	Scan(dest ...interface{}) error
	Err() error
	Close() error
}

func getLimit(la, lb int) int {
	if lb > 0 {
		if la > 0 {
			if la > lb {
				la = lb
			}
		} else {
			la = lb
		}
	}
	return la
}

// DataScan
// v should be a pointer type.
// Support type:
//  Base Type
//  struct
//  *struct
//  map[string]string
//  *map[string]string
//  map[string][]byte
//  *map[string][]byte
// List type:
//  []
//  [len]
// Example:
//  [100]map[string]string   Get 100 lines to map
//  map[string]string        Get 1 lines to map
//  []*struct                All to *struct
//  *[100]struct             Get 100 lines to struct
//
// var ret [100]map[string]string
// DataScan(key, data, &ret)
func RowsScan(rows Rows, v interface{}, limit int,
	fn func(reflect.StructField) string, f int) (int, error) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr {
		return 0, ErrNotPointer
	}
	val = val.Elem()
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		val = val.Elem()
	}

	l := 0
	switch val.Kind() {
	case reflect.Array:
		l = val.Len()
		if l == 1 {
			f = 0
		}
	case reflect.Slice:
		l = -1
	default:
		l = 1
		f = 0
	}

	l = getLimit(l, limit)

	if f == 0 {
		return rowsScanBytes(rows, v, l, fn)
	} else if f > 0 {
		return rowsScanChannel(rows, v, l, fn, f-1)
	}
	return 0, nil
}

// rowsLimit
func rowsLimit(rows Rows, limit int, g bool, df func(d [][]byte)) ([]string, error) {
	if limit == 0 {
		return nil, nil
	}
	if !rows.Next() {
		return nil, nil
	}
	key, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	keysize := len(key)
	if keysize == 0 {
		return nil, nil
	}

	ff := func() {
		for i := 0; i != limit; i++ {
			r := makeBytesInterface(keysize)
			if err := rows.Scan(r...); err != nil {
				fmt.Sprintln(err)
				break
			}

			df(rowsInterfaceToByte(r))
			if !rows.Next() {
				break
			}
		}
		df(nil)
	}
	if g {
		go ff()
	} else {
		ff()
	}

	return key, nil
}

// rowScanSlice row scan Slice
func rowScanSlice(key []string, d [][]byte, val reflect.Value) error {
	switch val.Kind() {
	default:
		return ErrInvalidType
	case reflect.Ptr:
		return rowScanSlice(key, d, val.Elem())
	case reflect.Slice:
		return rowScanSliceValue(key, d, val)
	}
	return nil
}

// rowScanSliceValue row scan Slice value
func rowScanSliceValue(key []string, d [][]byte, val reflect.Value) error {
	tt := val.Type()
	te := tt.Elem()
	switch te.Kind() {
	default:
		return ErrInvalidType
	case reflect.String:
		l := len(d)
		val.Set(reflect.MakeSlice(tt, l, l))
		for k, _ := range key {
			val.Index(k).Set(reflect.ValueOf(string(d[k])))
		}
	case reflect.Slice:
		if te.Elem().Kind() != reflect.Uint8 {
			return ErrInvalidType
		}
		val.Set(reflect.ValueOf(d))
	}
	return nil

}

// rowScanMap row scan Map
func rowScanMap(key []string, d [][]byte, val reflect.Value) error {
	switch val.Kind() {
	default:
		return ErrInvalidType
	case reflect.Ptr:
		return rowScanMap(key, d, val.Elem())
	case reflect.Map:
		return rowScanMapValue(key, d, val)
	}
	return nil
}

// rowScanMapValue row scan Map value
func rowScanMapValue(key []string, d [][]byte, val reflect.Value) error {
	tt := val.Type()
	if tt.Key().Kind() != reflect.String {
		return ErrInvalidType
	}
	val.Set(reflect.MakeMap(tt))
	te := tt.Elem()
	switch te.Kind() {
	default:
		return ErrInvalidType
	case reflect.String:
		for k, v := range key {
			val.SetMapIndex(reflect.ValueOf(v), reflect.ValueOf(string(d[k])))
		}
	case reflect.Slice:
		if te.Elem().Kind() != reflect.Uint8 {
			return ErrInvalidType
		}
		for k, v := range key {
			val.SetMapIndex(reflect.ValueOf(v), reflect.ValueOf(d[k]))
		}
	}
	return nil
}

// rowScanStruct row scan Struct
func rowScanStruct(key [][]string, d [][]byte, val reflect.Value) error {
	switch val.Kind() {
	default:
		return ErrInvalidType
	case reflect.Ptr:
		return rowScanStruct(key, d, val.Elem())
	case reflect.Struct:
		for k, v := range key {
			if len(v) == 0 {
				continue
			}

			fi := val
			for _, v0 := range v {
				fi = fi.FieldByName(v0)
			}
			fi = fi.Addr()
			if err := ConvertAssign(fi.Interface(), d[k]); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// rows2MapStrings rows to map string
func rows2MapStrings(key []string, data [][][]byte) []map[string]string {
	m := make([]map[string]string, 0, len(data))
	for _, v := range data {
		m = append(m, rows2MapString(key, v))
	}
	return m
}

// rows2MapString rows to map string
func rows2MapString(key []string, v [][]byte) map[string]string {
	m0 := map[string]string{}
	for i, k := range key {
		if vv := v[i]; len(vv) == 0 {
			m0[k] = ""
		} else {
			m0[k] = string(vv)
		}
	}
	return m0
}

// rows2Maps rows to map
func rows2Maps(key []string, data [][][]byte) []map[string][]byte {
	m := make([]map[string][]byte, 0, len(data))
	for _, v := range data {
		m = append(m, rows2Map(key, v))
	}
	return m
}

// rows2Map rows to map
func rows2Map(key []string, v [][]byte) map[string][]byte {
	m0 := map[string][]byte{}
	for i, k := range key {
		if vv := v[i]; len(vv) == 0 {
			m0[k] = []byte{}
		} else {
			m0[k] = vv
		}
	}

	return m0
}

// rows2Table rows to table
func rows2Table(key []string, data [][][]byte) [][]string {
	m := make([][]string, 0, len(data)+1)
	m = append(m, key)
	for _, v := range data {
		m0 := make([]string, 0, len(v))
		for _, v0 := range v {
			if len(v0) == 0 {
				m0 = append(m0, "")
			} else {
				m0 = append(m0, string(v0))
			}
		}
		m = append(m, m0)
	}
	return m
}

// makeBytesInterface returns the specified number of bytes interface arrays
func makeBytesInterface(max int) []interface{} {
	r := make([]interface{}, 0, max)
	for i := 0; i != max; i++ {
		r = append(r, &[]byte{})
	}
	return r
}

// rowsInterfaceToByte bytes interface arrays to bytes arrays
func rowsInterfaceToByte(m []interface{}) [][]byte {
	r0 := make([][]byte, 0, len(m))
	for _, v := range m {
		if v0, ok := v.(*[]byte); ok && v0 != nil {
			r0 = append(r0, *v0)
		} else {
			r0 = append(r0, []byte{})
		}
	}
	return r0
}

// rowsScanValueFunc returns the traversal function
func rowsScanValueFunc(tt reflect.Type, key []string, fn func(reflect.StructField) string) (func(key []string, d [][]byte, val reflect.Value) error, error) {
	switch tt.Kind() {
	default:
		return nil, ErrInvalidType
	case reflect.Struct:
		key0 := colAdjust(tt, key, fn)
		return func(key []string, d [][]byte, val reflect.Value) error {
			return rowScanStruct(key0, d, val)
		}, nil
	case reflect.Map:
		return rowScanMap, nil
	case reflect.Slice:
		return rowScanSlice, nil
	}
}
