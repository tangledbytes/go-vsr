package log

import "encoding/json"

type Log struct {
	Data      string `json:"data"`
	ClientID  uint64 `json:"client_id,string"`
	RequestID uint64 `json:"request_id,string"`
}

type Logs []Log

func NewLogs() Logs {
	return make(Logs, 0)
}

func (l Logs) Len() int {
	return len(l)
}

func (l *Logs) Add(log Log) {
	*l = append(*l, log)
}

func (l Logs) OpAt(i int) string {
	return (l)[i-1].Data
}

func (l Logs) ClientIDAt(i int) uint64 {
	return (l)[i-1].ClientID
}

func (l Logs) RequestIDAt(i int) uint64 {
	return (l)[i-1].RequestID
}

func (l *Logs) UnmarshalJSON(data []byte) error {
	var logs []Log
	if err := json.Unmarshal(data, &logs); err != nil {
		return err
	}

	*l = logs
	return nil
}

func (l Logs) MarshalJSON() ([]byte, error) {
	return json.Marshal([]Log(l))
}
