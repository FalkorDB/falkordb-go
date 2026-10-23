package falkordb

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
)

// normalize converts a Go value into one of the types ToString handles:
// nil, bool, int64, float64, string, []interface{} or map[string]interface{}.
func normalize(i interface{}) (interface{}, error) {
	if i == nil {
		return nil, nil
	}

	v := reflect.ValueOf(i)
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := v.Uint()
		if u > math.MaxInt64 {
			return nil, fmt.Errorf("value %d overflows int64", u)
		}
		return int64(u), nil
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if v.Kind() == reflect.Float32 {
			// go through the shortest float32 form so 1.1 stays 1.1
			f, _ = strconv.ParseFloat(strconv.FormatFloat(f, 'g', -1, 32), 64)
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("unsupported value %v", f)
		}
		return f, nil
	case reflect.String:
		return v.String(), nil
	case reflect.Slice, reflect.Array:
		arr := make([]interface{}, v.Len())
		for j := range arr {
			n, err := normalize(v.Index(j).Interface())
			if err != nil {
				return nil, err
			}
			arr[j] = n
		}
		return arr, nil
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("unsupported map key type %s", v.Type().Key())
		}
		data := make(map[string]interface{}, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			n, err := normalize(iter.Value().Interface())
			if err != nil {
				return nil, err
			}
			data[iter.Key().String()] = n
		}
		return data, nil
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil, nil
		}
		return normalize(v.Elem().Interface())
	default:
		return nil, fmt.Errorf("unsupported type %T", i)
	}
}

// buildParamsHeader is BuildParamsHeader returning an error for values that cannot be sent
func buildParamsHeader(params map[string]interface{}) (string, error) {
	header := "CYPHER "
	for key, value := range params {
		n, err := normalize(value)
		if err != nil {
			return "", fmt.Errorf("param %q: %w", key, err)
		}
		header += fmt.Sprintf("%s=%v ", key, ToString(n))
	}
	return header, nil
}
