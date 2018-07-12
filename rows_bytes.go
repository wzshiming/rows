package rows

import "reflect"

// DataScanBytes fill in key and data to v
func DataScanBytes(key []string, data [][][]byte, v interface{}, fn func(reflect.StructField) string) (int, error) {
	if len(data) == 0 || len(key) == 0 {
		return 0, nil
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
	rows.Close()

	if err != nil {
		return nil, nil, err
	}
	return key, data, nil
}

// RowsScanBytes fill in rows to v
func RowsScanBytes(rows Rows, v interface{}, limit int,
	fn func(reflect.StructField) string) (int, error) {
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
	case reflect.Slice:
		l = -1
	default:
		l = 1
	}

	l = getLimit(l, limit)

	return rowsScanBytes(rows, v, l, fn)
}

// rowsScanBytes fill in rows to v
func rowsScanBytes(rows Rows, v interface{}, limit int,
	fn func(reflect.StructField) string) (int, error) {
	key, data, err := RowsLimitBytes(rows, limit)
	if err != nil {
		return 0, err
	}

	return DataScanBytes(key, data, v, fn)
}

// rowsScanValue rows scan value
func rowsScanValueBytes(key []string, data [][][]byte, val reflect.Value, fn func(reflect.StructField) string) (int, error) {
	tt := val.Type().Elem()
	ps := 0
	for tt.Kind() == reflect.Ptr {
		tt = tt.Elem()
		ps++
	}

	rs, err := rowsScanValueFunc(tt, key, fn)
	if err != nil {
		return 0, err
	}

	if val.Len() == 0 {
		if val.Kind() == reflect.Slice {
			val.Set(reflect.MakeSlice(val.Type(), len(data), len(data)))
		} else {
			return 0, nil
		}
	}

	for k, v := range data {
		d := reflect.New(tt).Elem()
		if err := rs(key, v, d); err != nil {
			return 0, err
		}
		for i := 0; i != ps; i++ {
			d = d.Addr()
		}
		val.Index(k).Set(d)
	}
	return val.Len(), nil
}

// rowsScanValuesBytes rows scan values
func rowsScanValuesBytes(key []string, data [][][]byte, val reflect.Value, fn func(reflect.StructField) string) (int, error) {
	switch val.Kind() {
	default:
		return 0, ErrInvalidType
	case reflect.Ptr:
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		return rowsScanValuesBytes(key, data, val.Elem(), fn)
	case reflect.Slice:
		fallthrough
	case reflect.Array:
		return rowsScanValueBytes(key, data, val, fn)
	case reflect.Struct:
		key0 := colAdjust(val.Type(), key, fn)
		return 1, rowScanStruct(key0, data[0], val)
	case reflect.Map:
		return 1, rowScanMap(key, data[0], val)
	}
}
