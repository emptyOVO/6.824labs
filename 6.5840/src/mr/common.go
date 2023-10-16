package mr
import(
	"fmt"
	"log"
)

// DEBUG indicate the debug mode

const DEBUG = true
type TaskPhase int

//MapPhase & ReducePhase
const(
	MapPhase TaskPhase = 0
	ReducePhase TaskPhase =1
)

// Task
type Task struct{
	FileName string
	NReduce int
	NMap int
	Seq int  //index
	Phase TaskPhase //Map or reduce
	Alive bool
}

func DPrintf(format string,v ...interface{}){
	if(DEBUG){
		log.Printf(format+"\n",v...)
	}
}

func reduceName(mapIdx,reduceIdx int)string{
	return fmt.Sprintf("mr-%d-%d",mapIdx,reduceIdx)
}

func outputName(reduceIdx int)string{
	return fmt.Sprintf("mr-out-%d",reduceIdx)
}



