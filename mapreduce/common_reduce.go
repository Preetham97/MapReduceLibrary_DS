package mapreduce

import (
	//"io"
	//"os"
	//  "encoding/json"
	//"log"

	"encoding/json"
	//"fmt"
	"io"
	"log"
	"os"
	"sort"
	//"time"
)



func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	//t := time.Now()

	sortmap := make(map[string][]string)
	var keys [] string


	for  i := 0; i < nMap ; i++ {

		Filename := reduceName(jobName, i, reduceTask)
		file, err := os.Open(Filename)

	   	if err != nil {
			   log.Fatal("opening file in reduce job: %v ",err);
		 }

	   	dec := json.NewDecoder(file)

	    for  {
	   	      var K KeyValue
	   	      err := dec.Decode(&K)
	   	      if err == io.EOF {
						 break
					 }
	   	      //intermediateKeys = append(intermediateKeys,K)
	   	    keys = append(keys,K.Key)
		    sortmap[K.Key] = append(sortmap[K.Key],K.Value)
	   }

	   	// does map guarantee order ?
	/*  for _,kv := range intermediateKeys {
            sortmap[kv.Key] = append(sortmap[kv.Key],kv.Value)
	  } */

	}

	sort.Strings(keys)
	//for k := 0;k < 11; k++ {
	//	fmt.Println(keys[k])
	//}
	out,err := os.OpenFile(outFile,os.O_RDWR|os.O_CREATE,0755)
	if err != nil {
		log.Fatal("writing reduce output to the final output file: %v ", err)
	}

	//r1 := time.Now()
	//fmt.Println(r1.Format("20060102150405"))

	for _,k := range keys {
		output := reduceF(k,sortmap[k])
		outputKV := KeyValue{k,output}
		enc := json.NewEncoder(out)
		error := enc.Encode(&outputKV)
		if error != nil {
			log.Fatal(error)
		}

	}
	out.Close()

	//r2 := time.Now()
	//fmt.Println(r2.Format("20060102150405"))






}
