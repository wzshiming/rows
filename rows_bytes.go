package rows

import "reflect"

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
func DataScanBytes(key []string, data [][][]byte, v interface{}, fn func(reflect.StructField) string) error {
	if len(data) == 0 || len(key) == 0 {
		return nil
	}

	val := reflect.ValueOf(v)
	return rowsScanValuesBytes(key, data, val, fn)
}

// RowsLimitBytes
// if limit >= 0 Read maximum rows limit
// else < 0 Not limited
func RowsLimitBytes(rows Rows, limit int) ([]string, [][][]byte, error) {

	ms := 1024
	if limit > 0 {
		ms = limit
	}
	data := make([][][]byte, 0, ms)

	key, err := rowsLimit(rows, limit, false, func(d [][]byte) {
		if d != nil {
			data = append(data, d)
		}
	})
	if err != nil {
		return nil, nil, err
	}
	return key, data, nil
}

func RowsScanBytes(rows Rows, v interface{}, fn func(reflect.StructField) string) (int, error) {
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
	key, data, err := RowsLimitBytes(rows, limit)
	if err != nil {
		return 0, err
	}

	err = DataScanBytes(key, data, v, fn)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// rowsScanValue rows scan value
func rowsScanValueBytes(key []string, data [][][]byte, val reflect.Value, fn func(reflect.StructField) string) error {
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

	rs, err := rowsScanValueFunc(tt, key, fn)
	if err != nil {
		return err
	}

	for k, v := range data[:ml] {
		d := reflect.New(tt).Elem()
		if err := rs(key, v, d); err != nil {
			return err
		}
		for i := 0; i != ps; i++ {
			d = d.Addr()
		}
		val.Index(k).Set(d)
	}
	return nil
}

// rowsScanValuesBytes rows scan values
func rowsScanValuesBytes(key []string, data [][][]byte, val reflect.Value, fn func(reflect.StructField) string) error {
	switch val.Kind() {
	default:
		return ErrInvalidType
	case reflect.Ptr:
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		return rowsScanValuesBytes(key, data, val.Elem(), fn)
	case reflect.Slice:
		l := len(data)
		val.Set(reflect.MakeSlice(val.Type(), l, l))
		fallthrough
	case reflect.Array:
		return rowsScanValueBytes(key, data, val, fn)
	case reflect.Struct:
		key0 := colAdjust(val.Type(), key, fn)
		return rowScanStruct(key0, data[0], val)
	case reflect.Map:
		return rowScanMap(key, data[0], val)
	}
}
