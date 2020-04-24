package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
)

// datastore provider provides Datastore Service.

type DataStore interface {
	store(str string, day time.Time)
}

var (
	//	nodesrv   = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	noalive = flag.Bool("noalive", false, "Not output alive data")
	//	local     = flag.String("local", "", "Local Synerex Server")
	sendfile  = flag.String("sendfile", "", "Sending file name") // only one file
	startDate = flag.String("startDate", "02-07", "Specify Start Date")
	endDate   = flag.String("endDate", "12-31", "Specify End Date")
	startTime = flag.String("startTime", "00:00", "Specify Start Time")
	endTime   = flag.String("endTime", "24:00", "Specify End Time")
	dir       = flag.String("dir", "", "Directory of data storage") // for all file
	saveDir   = flag.String("sdir", "save", "Directory for store data")
	all       = flag.Bool("all", false, "Send all file in Dir") // for all file
	speed     = flag.Float64("speed", 1.0, "Speed of sending packets")
	//	multi     = flag.Int("multi", 1, "only default works for filter") // only 1 works
	mu      sync.Mutex
	version = "0.01"
	//	baseDir   = "store"
	dataDir string
	ds      DataStore
	today   time.Time
)

func init() {
	flag.Parse()

	var err error
	dataDir, err = os.Getwd()
	if err != nil {
		fmt.Printf("Can't obtain current wd")
	}
	dataDir = filepath.ToSlash(dataDir) + "/" + *saveDir
	ds = &FileSystemDataStore{
		storeDir: dataDir,
	}
}

type FileSystemDataStore struct {
	storeDir  string
	storeFile *os.File
	todayStr  string
}

// open file with today info
func (fs *FileSystemDataStore) store(str string, day time.Time) {
	const layout = "2006-01-02"
	//	day := time.Now()
	todayStr := day.Format(layout) + ".csv"
	if fs.todayStr != "" && fs.todayStr != todayStr {
		fs.storeFile.Close()
		fs.storeFile = nil
	}
	if fs.storeFile == nil {
		_, er := os.Stat(fs.storeDir)
		if er != nil { // create dir
			er = os.MkdirAll(fs.storeDir, 0777)
			if er != nil {
				fmt.Printf("Can't make dir '%s'.", fs.storeDir)
				return
			}
		}
		fs.todayStr = todayStr
		file, err := os.OpenFile(filepath.FromSlash(fs.storeDir+"/"+todayStr), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("Can't open file '%s'", todayStr)
			return
		}
		fs.storeFile = file
	}
	fs.storeFile.WriteString(str + "\n")
}

const dateFmt = "2006-01-02T15:04:05.999Z"

func atoUint(s string) uint32 {
	r, err := strconv.Atoi(s)
	if err != nil {
		log.Print("err", err)
	}
	return uint32(r)
}

func getHourMin(dt string) (hour int, min int) {
	st := strings.Split(dt, ":")
	hour, _ = strconv.Atoi(st[0])
	min, _ = strconv.Atoi(st[1])
	return hour, min
}

func getMonthDate(dt string) (month int, date int) {
	st := strings.Split(dt, "-")
	month, _ = strconv.Atoi(st[0])
	date, _ = strconv.Atoi(st[1])
	return month, date
}

// savig People Counter File.
func savePCounterFile() {
	lineCount := 0
	var year, month, date int
	bname := filepath.Base(*sendfile)
	fmt.Sscanf(bname, "%4d-%02d-%02d.csv", &year, &month, &date)
	log.Printf("%s Got %d, %d, %d", bname, year, month, date)
	today = time.Date(year, time.Month(month), date, 0, 0, 0, 0, time.UTC)
	// file
	fp, err := os.Open(*sendfile)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	started := false // start flag
	stHour, stMin := getHourMin(*startTime)
	edHour, edMin := getHourMin(*endTime)

	scanner := bufio.NewScanner(fp) // csv reader

	for scanner.Scan() { // read one line.
		dt := scanner.Text()
		token := strings.Split(dt, ",")
		tm, _ := time.Parse(dateFmt, token[0]) // RFC3339Nano
		//need to check time.
		if !started {
			if (tm.Hour() > stHour || (tm.Hour() == stHour && tm.Minute() >= stMin)) &&
				(tm.Hour() < edHour || (tm.Hour() == edHour && tm.Minute() <= edMin)) {
				started = true
				log.Printf("Start output! %v", tm)
			} else {
				continue // skip all data
			}
		} else {
			if tm.Hour() > edHour || (tm.Hour() == edHour && tm.Minute() > edMin) {
				started = false
				log.Printf("Stop  output! %v", tm)
				continue
			}
		}
		tp, _ := ptypes.TimestampProto(tm)
		ts0 := ptypes.TimestampString(tp)
		var ld string
		if len(token) < 5 {
			log.Printf("at Line %d Short len %d message:%s", lineCount, len(token), dt)
			continue
		}

		if len(token) == 5 {
			if len(token[1]) < 9 {
				log.Printf("at Line %d format error message:%s", lineCount, dt)
				continue
			} else if token[1][2:8] == "-vc3d-" { // might be different
				ld = fmt.Sprintf("%s,%s,%s,%s,%s", ts0, token[1], token[2], token[3], token[4])
			}
		} else {
			switch token[3] {
			case "alive": // may not required
				if *noalive {
					continue
				} else {
					ld = fmt.Sprintf("%s,%s,%s,alive,,", ts0, token[1], token[2])
				}
			case "statusList":
				if len(token) < 6 {
					log.Printf("Can't handle message %s at line %d", token[3], lineCount)
					log.Printf("Org:%s", dt)
				} else {
					ld = fmt.Sprintf("%s,%s,%s,statusList,%s,%s", ts0, token[1], token[2], token[4], token[5])
				}
			case "counter":
				if len(token) < 7 {
					log.Printf("Can't handle message %s at line %d", token[3], lineCount)
					log.Printf("Org:%s", dt)
				} else {
					ld = fmt.Sprintf("%s,%s,%s,counter,%s,%s,%s", ts0, token[1], token[2], token[4], token[5], token[6])
				}
			case "fillLevel":
				if len(token) < 6 {
					log.Printf("Can't handle message %s at line %d", token[3], lineCount)
					log.Printf("Org:%s", dt)
				} else {
					ld = fmt.Sprintf("%s,%s,%s,fillLevel,%s,%s", ts0, token[1], token[2], token[4], token[5])
				}
			case "dwellTime":
				if len(token) < 10 {
					log.Printf("Can't handle message %s at line %d", token[3], lineCount)
					log.Printf("Org:%s", dt)
				} else {
					ld = fmt.Sprintf("%s,%s,%s,dwellTime,%s,%s,%s,%s,%s,%s", ts0, token[1], token[2], token[4], token[5], token[6], token[7], token[8], token[9])
				}
			default: // this should not come
				log.Printf("Can't handle message %s at line %d", token[3], lineCount)
				log.Printf("Org:%s", dt)
			}
		}
		if ld == dt {
			ds.store(ld, today)
			lineCount++
		} else {
			log.Printf("Different Stirng!")
			log.Printf("Org:%s", dt)
			log.Printf("Fmt:%s", ld)
		}
	}
}

func saveAllPCounterFile() {
	// check all files in dir.
	stMonth, stDate := getMonthDate(*startDate)
	edMonth, edDate := getMonthDate(*endDate)

	if *dir == "" {
		log.Printf("Please specify directory")
		data := "data"
		dir = &data
	}
	files, err := ioutil.ReadDir(*dir)

	if err != nil {
		log.Printf("Can't open diretory %v", err)
		os.Exit(1)
	}
	// should be sorted.
	var ss = make(sort.StringSlice, 0, len(files))

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") { // check is CSV file
			//
			fn := file.Name()
			var year, month, date int
			ct, err := fmt.Sscanf(fn, "%4d-%02d-%02d.csv", &year, &month, &date)
			if (month > stMonth || (month == stMonth && date >= stDate)) &&
				(month < edMonth || (month == edMonth && date <= edDate)) {
				ss = append(ss, fn)
			} else {
				log.Printf("file: %d %v %s: %04d-%02d-%02d", ct, err, fn, year, month, date)
			}
		}
	}

	ss.Sort()

	for _, fname := range ss {
		dfile := path.Join(*dir, fname)
		// check start date.

		log.Printf("Sending %s", dfile)
		sendfile = &dfile
		savePCounterFile()
	}

}

//dataServer(pc_client)

func main() {
	flag.Parse()

	if *all { // send all file
		saveAllPCounterFile()
	} else if *dir != "" {
	} else if *sendfile != "" {
		//		for { // infinite loop..
		savePCounterFile()
		//		}
	}

	//	wg.Wait()

}
