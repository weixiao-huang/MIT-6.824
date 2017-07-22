package mapreduce

import (
	"os"
	"encoding/json"
	"log"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	// define intermediate file array, each element is a struct,
	// which saves a file pointer and a json decoder pointer
	var interFile []struct{
		file *os.File
		dec *json.Decoder
	}

	// define the result KeyValue map
	var resultKv map[string][]string
	resultKv = make(map[string][]string)

	for i := 0; i < nMap; i++ {
		// Get the intermediate file name according to the map index
		fn := reduceName(jobName, i, reduceTaskNumber)
		// Open file
		file, err := os.Open(fn)
		if err != nil {
			log.Println(err)
			return
		}
		// define the json decoder for getting the KeyValue data
		dec := json.NewDecoder(file)
		// Read file
		for {
			var kv KeyValue
			// Read every KeyValue data into kv
			err := dec.Decode(&kv)
			if err != nil { break }
			// Append the value sharing the same key
			resultKv[kv.Key] = append(resultKv[kv.Key], kv.Value)
		}
		interFile = append(interFile, struct {
			file *os.File
			dec  *json.Decoder
		}{ file: file, dec: dec })
	}

	// Create the output file
	file, err := os.Create(outFile)
	if err != nil {
		log.Println(err)
		return
	}
	// Create the json encoder for writing data into output file
	enc := json.NewEncoder(file)
	for key, values := range resultKv {
		// For each key, use reduceF to get the final KeyValue
		enc.Encode(KeyValue{key, reduceF(key, values)})
	}
	// Close the file
	file.Close()

	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
}
