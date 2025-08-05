package falkordb

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecord_GetByIndex_WhenNil(t *testing.T) {
	var record *Record
	_, err := record.GetByIndex(-1)
	if !errors.Is(err, ErrRecordNoValue) {
		assert.FailNow(t, err.Error())
	}
}

func TestRecord_GetByIndex(t *testing.T) {
	type fields struct {
		values []interface{}
		keys   []string
	}
	type args struct {
		index int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "GetByIndex where record is",
			fields: fields{
				keys: []string{
					"foo",
					"baz",
				},
				values: []interface{}{
					"bar",
					"buzz",
				},
			},
			args: args{
				index: 1,
			},
			want:    "buzz",
			wantErr: assert.NoError,
		},
		{
			name: "GetByIndex valid index 0",
			fields: fields{
				keys:   []string{"foo"},
				values: []interface{}{"bar"},
			},
			args:    args{index: 0},
			want:    "bar",
			wantErr: assert.NoError,
		},
		{
			name: "GetByIndex out of bounds",
			fields: fields{
				keys:   []string{"foo"},
				values: []interface{}{"bar"},
			},
			args:    args{index: 5},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "GetByIndex negative index",
			fields: fields{
				keys:   []string{"foo"},
				values: []interface{}{"bar"},
			},
			args:    args{index: -1},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Record{
				values: tt.fields.values,
				keys:   tt.fields.keys,
			}
			got, err := r.GetByIndex(tt.args.index)
			if !tt.wantErr(t, err, fmt.Sprintf("GetByIndex(%v)", tt.args.index)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetByIndex(%v)", tt.args.index)
		})
	}
}
