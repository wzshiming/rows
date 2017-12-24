package rows

import (
	"go/ast"
	"reflect"
	"strings"
)

type Rows interface {
	Next() bool
	Columns() ([]string, error)
	Scan(dest ...interface{}) error
	Err() error
}

func RowsScan(rows Rows, v interface{}, fn func(reflect.StructField) string) (int, error) {
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

	limit := 0
	switch val.Kind() {
	case reflect.Array:
		limit = val.Len()
	case reflect.Slice:
		limit = -1
	default:
		limit = 1
	}
	key, data, err := RowsLimit(rows, limit)
	if err != nil {
		return 0, err
	}

	err = DataScan(key, data, v, fn)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// RowsLimit
// if limit >= 0 Read maximum rows limit
// else < 0 Not limited
func RowsLimit(rows Rows, limit int) ([]string, [][][]byte, error) {
	if limit == 0 {
		return nil, nil, nil
	}
	if !rows.Next() {
		return nil, nil, nil
	}
	key, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	keysize := len(key)
	if keysize == 0 {
		return nil, nil, nil
	}

	ms := 1024
	if limit > 0 {
		ms = limit
	}
	data := make([][][]byte, 0, ms)

	for i := 0; i != limit; i++ {
		r := makeBytesInterface(keysize)
		if err := rows.Scan(r...); err != nil {
			return nil, nil, err
		}
		data = append(data, rowsInterfaceToByte(r))
		if !rows.Next() {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return key, data, nil
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
func DataScan(key []string, data [][][]byte, v interface{}, fn func(reflect.StructField) string) error {
	if len(data) == 0 || len(key) == 0 {
		return nil
	}

	val := reflect.ValueOf(v)
	return rowsScanValues(key, data, val, fn)
}

// rowsScanValues rows scan values
func rowsScanValues(key []string, data [][][]byte, val reflect.Value, fn func(reflect.StructField) string) error {
	switch val.Kind() {
	default:
		return ErrInvalidType
	case reflect.Ptr:
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		return rowsScanValues(key, data, val.Elem(), fn)
	case reflect.Slice:
		l := len(data)
		val.Set(reflect.MakeSlice(val.Type(), l, l))
		fallthrough
	case reflect.Array:
		return rowsScanValue(key, data, val, fn)
	case reflect.Struct:
		key0 := colAdjust(val.Type(), key, fn)
		return rowScanStruct(key0, data[0], val)
	case reflect.Map:
		return rowScanMap(key, data[0], val)
	}
}

// rowsScanValue rows scan value
func rowsScanValue(key []string, data [][][]byte, val reflect.Value, fn func(reflect.StructField) string) error {
	tt := val.Type().Elem()
	ps := 0
	for tt.Kind() == reflect.Ptr {
		tt = tt.Elem()
		ps++
	}
	ml := val.Len()
	if len(data) < ml {
		ml = len(data)
	}
	switch tt.Kind() {
	default:
		return ErrInvalidType
	case reflect.Struct:
		key0 := colAdjust(tt, key, fn)
		for k, v := range data[:ml] {
			d := reflect.New(tt).Elem()

			if err := rowScanStruct(key0, v, d); err != nil {
				return err
			}

			for i := 0; i != ps; i++ {
				d = d.Addr()
			}
			val.Index(k).Set(d)
		}
	case reflect.Map:
		for k, v := range data[:ml] {
			d := reflect.New(tt).Elem()

			if err := rowScanMap(key, v, d); err != nil {
				return err
			}

			for i := 0; i != ps; i++ {
				d = d.Addr()
			}
			val.Index(k).Set(d)
		}

	case reflect.Slice:
		for k, v := range data[:ml] {
			d := reflect.New(tt).Elem()
			if err := rowScanSlice(key, v, d); err != nil {
				return err
			}
			for i := 0; i != ps; i++ {
				d = d.Addr()
			}
			val.Index(k).Set(d)
		}
	}
	return nil
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

func makeBytesInterface(max int) []interface{} {
	r := make([]interface{}, 0, max)
	for i := 0; i != max; i++ {
		r = append(r, &[]byte{})
	}
	return r
}

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

// colAdjust Adjust col
func colAdjust(tt reflect.Type, key []string, fn func(reflect.StructField) string) [][]string {
	m := map[string]int{}
	for i, v := range key {
		m[v] = i
	}
	rk := make([][]string, len(key))
	colAdjustMap(tt, key, nil, m, rk, fn)
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

func MakeFieldName(tag string) func(fn reflect.StructField) string {
	return func(fn reflect.StructField) string {
		b := fn.Tag.Get(tag)
		dd := strings.Split(b, ",")[0]
		if dd == "-" {
			return ""
		}
		if dd == "" {
			return Hump2Snake(fn.Name)
		}
		return dd
	}
}
