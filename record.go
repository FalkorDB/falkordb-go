package falkordb

import "fmt"

type Record struct {
	values []interface{}
	keys   []string
}

func recordNew(values []interface{}, keys []string) *Record {
	r := &Record{
		values: values,
		keys:   keys,
	}

	return r
}

func (r *Record) Keys() []string {
	if r == nil {
		return nil
	}

	return r.keys
}

func (r *Record) Values() []interface{} {
	if r == nil {
		return nil
	}

	return r.values
}

func (r *Record) Get(key string) (interface{}, bool) {
	// TODO: switch from []string to map[string]int
	for i := range r.keys {
		if r.keys[i] == key {
			return r.values[i], true
		}
	}
	return nil, false
}

func (r *Record) GetByIndex(index int) (interface{}, error) {
	if r == nil {
		return nil, fmt.Errorf("record is nil: %w", ErrRecordNoValue)
	}

	if index >= len(r.values) {
		return nil, ErrRecordNoValue
	}

	return r.values[index], nil
}
