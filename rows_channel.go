package rows

import (
	"reflect"

	"github.com/wzshiming/fork"
)

var (
	MaxForkSize  = 16
	MakeSliceCap = 1024 * 16
	MaxBuffer    = 1024
)

func DataScanChannel(key []string, data chan [][]byte, v interface{},
	fn func(reflect.StructField) string, f int) error {
	if len(key) == 0 {
		return nil
	}

	val := reflect.ValueOf(v)
	return rowsScanValuesChannel(key, data, val, fn, f)
}

// RowsLimitChannel
// if limit >= 0 Read maximum rows limit
// else < 0 Not limited
func RowsLimitChannel(rows Rows, limit int) ([]string, chan [][]byte, error) {
	data := make(chan [][]byte, MaxBuffer)
	key, err := rowsLimit(rows, limit, true, func(d [][]byte) {
		if d != nil {
			data <- d
		} else {
			close(data)
		}
	})
	if err != nil {
		return nil, nil, err
	}
	return key, data, nil
}

func RowsScanChannel(rows Rows, v interface{},
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

	limit := 0
	switch val.Kind() {
	case reflect.Array:
		limit = val.Len()
	case reflect.Slice:
		limit = -1
	default:
		limit = 1
	}
	return rowsScanChannel(rows, v, limit, fn, f)
}

func rowsScanChannel(rows Rows, v interface{}, limit int,
	fn func(reflect.StructField) string, f int) (int, error) {
	key, data, err := RowsLimitChannel(rows, limit)
	if err != nil {
		return 0, err
	}

	if f--; f < 1 {
		f = 1
	}
	err = DataScanChannel(key, data, v, fn, f)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// rowsScanValueChannel rows scan value
func rowsScanValueChannel(key []string, data chan [][]byte, val reflect.Value,
	fn func(reflect.StructField) string, f int) error {
	tt := val.Type().Elem()
	ps := 0
	for tt.Kind() == reflect.Ptr {
		tt = tt.Elem()
		ps++
	}

	rs, err := rowsScanValueFunc(tt, key, fn)
	if err != nil {
		return err
	}

	if val.Len() == 0 {
		if val.Kind() == reflect.Slice {
			val.Set(reflect.MakeSlice(val.Type(), 1, MakeSliceCap))
		} else {
			return nil
		}
	}

	var forks *fork.Fork
	if f > 1 {
		forks = fork.NewForkBuf(f, f*10)
	}
	k := 0
	for v := range data {
		if val.Len() == k {
			if val.Kind() == reflect.Slice {
				val.Set(reflect.AppendSlice(val, val))
			} else {
				break
			}
		}

		func(k int, v [][]byte) {
			ff := func() {
				d := reflect.New(tt).Elem()
				if err := rs(key, v, d); err != nil {
					return
				}

				for i := 0; i != ps; i++ {
					d = d.Addr()
				}
				val.Index(k).Set(d)
			}
			if forks != nil {
				forks.Push(ff)
			} else {
				ff()
			}

		}(k, v)
		k++
	}
	if forks != nil {
		forks.JoinMerge()
	}
	if val.Kind() == reflect.Slice {
		val.Set(val.Slice(0, k))
	}
	return nil
}

// rowsScanValuesChannel rows scan values
func rowsScanValuesChannel(key []string, data chan [][]byte, val reflect.Value,
	fn func(reflect.StructField) string, f int) error {
	switch val.Kind() {
	default:
		return ErrInvalidType
	case reflect.Ptr:
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		return rowsScanValuesChannel(key, data, val.Elem(), fn, f)
	case reflect.Slice:
		fallthrough
	case reflect.Array:
		return rowsScanValueChannel(key, data, val, fn, f)
	case reflect.Struct:
		key0 := colAdjust(val.Type(), key, fn)
		return rowScanStruct(key0, <-data, val)
	case reflect.Map:
		return rowScanMap(key, <-data, val)
	}
}
