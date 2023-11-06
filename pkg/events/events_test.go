package events

import (
	"bytes"
	"io"
	"math"
	"reflect"
	"testing"
)

func TestEvent_FromReader(t *testing.T) {
	type args struct {
		r io.Reader
	}
	tests := []struct {
		name     string
		expevent Event
		args     args
		wantErr  bool
	}{
		{
			name: "Invalid JSON",
			expevent: Event{
				Type: EventRequest,
				Data: Request{
					ID:       1,
					ClientID: 10,
					Op:       "hello",
				},
			},
			args: args{
				r: bytes.NewReader([]byte(`{"@type":1,"data":{`)),
			},
			wantErr: true,
		},
		{
			name: "Valid JSON",
			expevent: Event{
				Type: EventRequest,
				Data: Request{
					ID:       1,
					ClientID: 10,
					Op:       "test command",
				},
			},
			args: args{
				r: bytes.NewReader([]byte(`{"@type":1,"data":{"id":"1","client_id":"10","op":"test command"}}`)),
			},
			wantErr: false,
		},
		{
			name: "Valid JSON with newline terminator",
			expevent: Event{
				Type: EventRequest,
				Data: Request{
					ID:       1,
					ClientID: 10,
					Op:       "test command",
				},
			},
			args: args{
				r: bytes.NewReader([]byte(`{"@type":1,"data":{"id":"1","client_id":"10","op":"test command"}}` + "\n")),
			},
			wantErr: false,
		},
		{
			name: "Valid JSON with max UINT64",
			expevent: Event{
				Type: EventRequest,
				Data: Request{
					ID:       math.MaxUint64,
					ClientID: 101,
					Op:       "test command",
				},
			},
			args: args{
				r: bytes.NewReader([]byte(`{"@type":1,"data":{"id":"18446744073709551615","client_id":"101","op":"test command"}}`)),
			},
			wantErr: false,
		},
		{
			name: "Valid \"PREPARE\" JSON with max UINT64",
			expevent: Event{
				Type: EventPrepare,
				Data: Prepare{
					ViewNum:   3,
					OpNum:     math.MaxUint64,
					CommitNum: 101,
					Request: Request{
						ID:       12,
						ClientID: 101,
						Op:       "test command",
					},
				},
			},
			args: args{
				r: bytes.NewReader([]byte(`{"@type":3,"data":{"view_num":"3","op_num":"18446744073709551615","commit_num":"101","request": {"id": "12", "client_id": "101", "op": "test command"}}}`)),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := Event{}
			err := ev.FromReader(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Event.FromReader() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if !reflect.DeepEqual(ev, tt.expevent) {
					t.Fatalf("Event.FromReader() got = %+v, want %+v", ev, tt.expevent)
				}
			}
		})
	}
}

func TestEvent_ToWriter(t *testing.T) {
	type fields struct {
		Type EventType
		Data any
	}
	tests := []struct {
		name    string
		fields  fields
		wantW   string
		wantErr bool
	}{
		{
			name: "Test 1",
			fields: fields{
				Type: EventRequest,
				Data: Request{
					ID:       1,
					ClientID: 10,
					Op:       "hello",
				},
			},
			wantW:   `{"@type":1,"data":{"id":"1","client_id":"10","op":"hello"}}` + "\n",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &Event{
				Type: tt.fields.Type,
				Data: tt.fields.Data,
			}
			w := bytes.NewBuffer([]byte{})
			if err := ev.ToWriter(w); (err != nil) != tt.wantErr {
				t.Errorf("Event.ToWriter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("Event.ToWriter() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}
