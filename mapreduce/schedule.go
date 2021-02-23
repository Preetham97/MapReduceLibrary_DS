package mapreduce

import (
	"fmt"
	"sync"
)


// citation:
// 1. Used the following URL to understand the syntax to call a function recursively in go lang
// https://stackoverflow.com/questions/28099441/define-a-recursive-function-within-a-function-in-go
// 2. used golang.org tutorials for understanding sync waitgroup.
//
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	//find a worker for each task.
	// questions for part 4
	// should we add failed workers to the list of available workers ..
	 var wg sync.WaitGroup
	 taskQ := make(chan string,ntasks)
     var recurseGo func(int)
	 recurseGo =  func(x int)  {
		defer wg.Done()
		var WorkerRPC string
		select {
		case  l:= <-taskQ:
			WorkerRPC = l
		case  m := <-registerChan:
			WorkerRPC = m
		}
		DotaskargsValues := DoTaskArgs{jobName,mapFiles[x],phase,x,n_other}
		fmt.Println(DotaskargsValues)
		output := call(WorkerRPC,"Worker.DoTask", DotaskargsValues,nil)
		if output {
			taskQ <- WorkerRPC
		} else
		{
			wg.Add(1)  // else - negative waitgroup
			go recurseGo(x)
		}
	}

	 for i := 0; i < ntasks ; i++ {

		    func (x int) {
				wg.Add(1)
		    	go recurseGo(x)
				}(i)
		    }


     wg.Wait()
	 fmt.Printf("Schedule: %v done\n", phase)

	 return
}
