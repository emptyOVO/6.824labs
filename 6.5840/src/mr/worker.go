package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (w *worker) run() {
	for {
		task := w.requestTask()
		if task == nil {
			DPrintf("Done!")
			break
		}
		t := *task
		if !t.Alive {
			DPrintf("task not alive,exit")
			return
		}
		w.doTask(t)
	}
}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	if ok := call("Coordinator.RegWorker", &args, &reply); !ok {
		log.Fatal("register failed")
	}
	w.id = reply.WorkerID
}

func (w *worker) requestTask() *Task {
	args := TaskArgs{}
	args.WorkerID = w.id
	reply := TaskReply{}

	if ok := call("Coordinator.GetOneTask", &args, &reply); !ok {
		DPrintf("worker get task failed,exit")
		os.Exit(1)
	}
	DPrintf("worker get task: %+v", reply.Task)
	return reply.Task
}

func (w *worker) doTask(t Task) {
	DPrintf("doing task")
	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase error: +%v", t))
	}
}

func (w *worker) doMapTask(t Task) {
	//read file
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}
	// do map task
	kvs := w.mapf(t.FileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	// write the result
	// 每一个map任务都产生nReduce个中间文件
	for idx, reduce := range reduces {
		fileName := reduceName(t.Seq, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range reduce {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)
}

func (w *worker) doReduceTask(t Task) {
	//scan the intermediate file called "mr-i-t.seq"
	//collect kvs by key where ihash(key) % nreduce == t.seq
	maps := make(map[string][]string)

	//iterate intermediate "mr-i-t.seq"
	for idx := 0; idx < t.NMap; idx++ {
		fileName := reduceName(idx, t.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}

		dec := json.NewDecoder(file)

		//read the file and put the kvs into maps by key
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	if err := ioutil.WriteFile(outputName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, nil)
	}

	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Print("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Seq = t.Seq
	args.Phase = t.Phase
	args.WorkerID = w.id
	reply := ReportTaskReply{}
	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		DPrintf("report task fail")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}*/

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
