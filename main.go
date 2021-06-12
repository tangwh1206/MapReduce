package main

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

const (
	TaskTypeUnDefine = 0
	TaskTypeMap = 1
	TaskTypeReduce = 2
)

const (
	TaskStatusInit = 1
	TaskStatusProcessing = 2
	TaskStatusDone = 3
	TaskStatusFailure = 4
)

type Task struct {
	ID  int64
	Type int64
	Status int64
	InFile string
}

type MapReduce struct {
	InputFilenames []string
	KeyToFileMap  map[string]*os.File
	TaskChan  chan *Task
	KVChan   chan *KeyValue
	ReduceTotal   int64
	MapTotal   int64
	MapDone    int64
	ReduceDone   int64
	mu sync.Mutex
}

func NewMapReduce(inFiles []string, nMap int64, nReduce int64) *MapReduce {
	return &MapReduce{
		InputFilenames: inFiles,
		KeyToFileMap:   make(map[string]*os.File),
		TaskChan:       make(chan *Task, 100),
		KVChan:         make(chan *KeyValue, 100),
		ReduceTotal:        nMap,
		MapTotal:           nReduce,
		MapDone: 0,
		ReduceDone: 0,
	}
}

func NewTask(id int64, _type int64, status int64, inFile string) *Task {
	return &Task{
		ID:     id,
		Type:   _type,
		Status: status,
		InFile: inFile,
	}
}

type KeyValue struct {
	Key string
	Value string
}

func Map(mr *MapReduce, key string, contents string) {
	defer atomic.AddInt64(&mr.MapDone, 1)
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)
	for _, w := range words {
		mr.KVChan <- &KeyValue{w, "1"}
	}
}

func Reduce(mr *MapReduce, key string, values []string) string {
	defer atomic.AddInt64(&mr.ReduceDone, 1)
	return strconv.Itoa(len(values))
}

func (mr *MapReduce) TaskConsumer() {
	for {
		select {
		case task:= <-mr.TaskChan:
			if task == nil {
				//TODO
				continue
			}
			task.Status = TaskStatusProcessing
			filename := task.InFile
			contents, err := ioutil.ReadFile(filename)
			if err != nil {
				//TODO
				continue
			}

			switch task.Type {
			case TaskTypeMap:
				go Map(mr, filename, string(contents))
			case TaskTypeReduce:
				lines := strings.Split(string(contents), "\n")
				if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
					lines = lines[:len(lines)-1]
				}
				go Reduce(mr, mr.GetKeyFromFile(filename), lines)
			default:
				//TODO
				continue
			}
		}
	}
}

func (mr *MapReduce) GenOutputFile(key string) string {
	return strings.Join([]string{"output", key}, "_") + ".txt"
}

func (mr *MapReduce) GetKeyFromFile(filename string) string {
	strs := strings.Split(filename, ".")
	strs = strings.Split(strs[0], "_")
	return strs[1]
}

func (mr *MapReduce) KVConsumer() {
	for {
		select {
		case kv := <- mr.KVChan:
			if kv == nil {
				//TODO
				continue
			}
			key := kv.Key
			val := kv.Value
			if file, ok := mr.KeyToFileMap[key]; !ok {
				filename := mr.GenOutputFile(key)
				var err error
				file, err = os.OpenFile(filename, os.O_CREATE | os.O_WRONLY |os.O_APPEND, 666)
				if err != nil {
					//TODO
					continue
				}
				mr.KeyToFileMap[key] = file
			}
			line := val + "\n"
			n, err := mr.KeyToFileMap[key].WriteString(line)
			if err != nil || n != len(line) {
				//TODO
				continue
			}
		}
	}
}

func (mr *MapReduce) MapTaskDone() bool {
	ret := false
	if mr.MapDone == mr.MapTotal {
		ret = true
	}
	return ret
}
func (mr *MapReduce) ReduceTaskDone() bool {
	ret := false
	if mr.ReduceDone == mr.ReduceTotal {
		ret = true
	}
	return ret
}

func main() {
	var inputFiles []string
	nMap := int64(len(inputFiles))
	//TODO：优化，改成协程池
	nReduce := int64(0)
	mr := NewMapReduce(inputFiles, nMap, nReduce)
	go mr.TaskConsumer()
	go mr.KVConsumer()
	tid := int64(1)
	for _, filename := range mr.InputFilenames {
		task := NewTask(tid, TaskTypeMap, TaskStatusInit, filename)
		mr.TaskChan <- task
		tid++
	}
	for {
		if mr.MapTaskDone() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, file := range mr.KeyToFileMap {
		task := NewTask(tid, TaskTypeReduce, TaskStatusInit, file.Name())
		mr.TaskChan <- task
		tid++
	}
	// update the number of key as the number of reduce task
	mr.ReduceTotal = int64(len(mr.KeyToFileMap))
	for {
		if mr.ReduceTaskDone() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}