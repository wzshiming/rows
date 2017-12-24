package rows

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

var (
	formatDatetime    = "2006-01-02 15:04:05"
	formatDate        = "2006-01-02"
	formatTime        = "15:04:05"
	formatTimeRFC3339 = time.RFC3339
)

// ConvertAssign copies to dest the value in src, converting it if possible.
// An error is returned if the copy would result in loss of information.
// dest should be a pointer type.
func ConvertAssign(dest interface{}, src []byte) error {
	if len(src) == 0 {
		return nil
	}
	if dest == nil {
		return ErrPointerNil
	}

	// Common cases, without reflect.
	switch d := dest.(type) {
	case *interface{}:
		*d = src
		return nil
	case *[]byte:
		*d = src
		return nil
	case *string:
		*d = string(src)
		return nil
	case *bool:
		*d, _ = strconv.ParseBool(string(src))
		return nil
	case *sql.RawBytes:
		*d = src
		return nil
	case *time.Time:
		s := string(src)
		ff := ""
		switch len(s) {
		case len(formatDatetime):
			ff = formatDatetime
			*d, _ = time.ParseInLocation(ff, s, time.Local)
			return nil
		case len(formatDate):
			ff = formatDate
			*d, _ = time.ParseInLocation(ff, s, time.Local)
			return nil
		case len(formatTime):
			ff = formatTime
			*d, _ = time.ParseInLocation(ff, s, time.Local)
			return nil
		case len(formatTimeRFC3339):
			*d, _ = time.Parse(formatTimeRFC3339, s)
			return nil
		default:
			return nil
		}
	case sql.Scanner:
		return d.Scan(src)
	}

	// The following conversions use a string value as an intermediate representation
	// to convert between various numeric types.
	//
	// This also allows scanning into user defined types such as "type Int int64".
	// For symmetry, also check for string destination types.
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr {
		return ErrNotPointer
	}
	dv = dv.Elem()
	switch dv.Kind() {
	case reflect.Ptr:
		dv.Set(reflect.New(dv.Type().Elem()))
		return ConvertAssign(dv.Interface(), src)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64, _ := strconv.ParseInt(string(src), 0, dv.Type().Bits())
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64, _ := strconv.ParseUint(string(src), 0, dv.Type().Bits())
		dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		f64, _ := strconv.ParseFloat(string(src), dv.Type().Bits())
		dv.SetFloat(f64)
		return nil
	case reflect.String:
		dv.SetString(string(src))
		return nil
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", src, dest)
}
