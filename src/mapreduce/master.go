package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  mappers := make(chan string, mr.nMap) // buffer size should equal to worker size
  reducers := make(chan string, mr.nReduce)
  completedMappers := make(chan int)
  completedReducers := make(chan int)

  //find workers and store in mappers and reducers
  go func(){
    for elem := range mr.registerChannel {
        //fmt.Println(elem)
        mappers <- elem
        reducers <- elem
    }
  }()
  println("hello world\n");
  //run mapper
  for i:=0; i < mr.nMap; i++ {
      args := new(DoJobArgs)
      args.File = mr.file
      args.Operation = Map
      args.JobNumber = i
      args.NumOtherPhase = mr.nReduce
      go func(){
          mapper := <- mappers
          var reply DoJobReply
          ok := call(mapper, "Worker.DoJob", args, &reply)
          for ok == false{
              fmt.Printf("Mapper: worker:%s job:%d error\n", mapper, args.JobNumber)
              //worker fail
              mapper := <- mappers
              ok = call(mapper, "Worker.DoJob", args, &reply)
              if ok == false{
                 fmt.Printf("fail again!!!")    
              }else {
                 mappers <- mapper
                 completedMappers <- args.JobNumber
                 return 
              }
          }
          mappers <- mapper
          completedMappers <- args.JobNumber
      }()
  }
  for i:=0; i < mr.nMap; i++ {
      <- completedMappers
  }
  println("map phase done")
  //run reducer
  for i:=0; i < mr.nReduce; i++ {
      //go func(){
          args := new(DoJobArgs)
          args.File = mr.file
          args.Operation = Reduce
          args.NumOtherPhase = mr.nMap
          args.JobNumber = i
      go func(){    
          reducer := <- reducers
          reply := new(DoJobReply)
          ok := call(reducer, "Worker.DoJob", args, &reply)
          for ok == false{
              fmt.Printf("Reducer: worker:%s job:%d error\n", reducer, args.JobNumber)
              //worker fail
              reducer := <- reducers
              ok = call(reducer, "Worker.DoJob", args, &reply)
              if ok == false{
                 fmt.Printf("fail again!!!")    
              } else { 
                 reducers <- reducer
                 completedReducers <- args.JobNumber
                 return 
              }
          }
          reducers <- reducer
          completedReducers <- args.JobNumber
      }()
  }
  for i:=0; i < mr.nReduce; i++{
      <- completedReducers
  }
  return mr.KillWorkers()
}
