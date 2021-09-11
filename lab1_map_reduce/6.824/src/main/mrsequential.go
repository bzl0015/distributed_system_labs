package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.824/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"
import "strconv"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])    // bh: expose map and reduce function from wc.so which was built with <-buildmode=plugin>

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	// bh: record all appeared words(keyvalue{word, 1}) into an array

	intermediate := []mr.KeyValue{}    // bh: array of keyvalue type from mr package
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))    // bh: filename doesn't matter
		intermediate = append(intermediate, kva...)    // bh: append a slice to slice use append(<slice1>. <slice2>...)
	}
	/*bh: paste map function from wc.go for reference
	func <func_name>(<func_param>) <return_type> {...}
	func Map(filename string, contents string) []mr.KeyValue {
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }
	
		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)    // bh: slice string based on ff function
	
		kva := []mr.KeyValue{}
		for _, w := range words {
			kv := mr.KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}
	*/
	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	fmt.Printf("bh_dbg: start processing...\n")
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {     // bh: find how many words are identical
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		for s:=0; s < len(values); s++ {
			digit, err := strconv.Atoi(values[s])
			//fmt.Printf("bh_dbg: here detects non zero value: %d and error: %s...\n", digit, err)
			if digit != 1 {
				fmt.Printf("bh_dbg: here detects non one value: %d and error: %s...\n", digit, err)
			}
		}
		output := reducef(intermediate[i].Key, values)    // bh: values is [1, 1, 1, 1 ...], goes to reduce function 
		/* bh: reduce simply count the length of the array.
		func Reduce(key string, values []string) string {
			// return the number of occurrences of this word.
			return strconv.Itoa(len(values))
		}
		*/
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
