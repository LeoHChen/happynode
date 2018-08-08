// Package client is the p2p transportation client.
// It supports sending/receiving data in json format.
// It also registers with server node and retrieve the adjacent nodes.
// It sends notification to server node once a data/message is received.
// It is designed to use RESTful API using http protocol as the baseline.
//
// API supported by the client
// POST /data     receive data from other nodes/clients
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

import (
	"github.com/LeoHChen/happynode/node"
)

// Notification is the json structure client sent to the server.
// It consists of node id, filename of the message as the unique identifier of the message,
// and timestamp on when the message is received.
type notification struct {
	Node      string `json:"node"`
	FileName  string `json:"file"`
	Timestamp string `json:"ts"`
}

// String function implements the String interface for printing out the Notification structure
func (n notification) String() string {
	return fmt.Sprintf(`{ "node":"%s", "file":"%s", "ts":"%s" }`, n.Node, n.FileName, n.Timestamp)
}

// P2PData is the data structure exchanged among clients.
// It consists of the filename, which is designed to the unique identifier of the message.
// And the sha1sum of the content of data.
type P2PData struct {
	FileName string `json:"filename"`
	ShaOne   string `json:"sha1"`
	Data     string `json:"data"`
}

// recordT is the structure to hold the transaction record of messages exchanged among clients.
// It consists of list of filename/message sent to other client.
type recordT struct {
	transaction []string
	timestamp   []time.Time
}

// The filename of the node database containing a list of adjacent clients.
const (
	nodeFileName = "adj_nodes.json"
)

// msgRecord is structure to keep track of messages received by the client.
type msgRecord struct {
	FileName  string
	ShaOne    string
	Timestamp time.Time
}

var (
	Debug *log.Logger
	Trace *log.Logger
	Error *log.Logger

	// in memory representation of adjacent nodes
	allNodes node.NodesType

	// client configuration data
	myIP     string
	logDir   string
	myID     uint64
	myClient *http.Client
	myServer string
	p2pPort  uint

	// transaction ledger to keep track of the messages sent to adjacent nodes, key is node id
	Ledger map[string]*recordT

	// notification record to keep track of messages received by client, key is the filename, value is the timestamp
	MsgList map[string]*msgRecord
)

// initLogger initalize the three different loggers.
// Debug is for debugging purose with more details.
// Trace is to keep track of important API calls.
// Error will capture the errors, but may not be fatal.
func initLogger(debugLog io.Writer, traceLog io.Writer, errorLog io.Writer) {
	Debug = log.New(debugLog, "DEBUG: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	Trace = log.New(traceLog, "TRACE: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	Error = log.New(errorLog, "ERROR: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
}

// getAdjacentNodes send a GET /node request to server to retrieve a list of adjacent clients.
// It is called every 30 seconds to keep updated on the latest list.
// It saves the Nodes structure to a file.
// TODO: save to disk only when Nodes structure changed.
func getAdjacentNodes() error {
	url := fmt.Sprintf("%s/node?id=%d", myServer, myID)

	response, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("GET node %s", err)
	}
	if err != nil {
		return fmt.Errorf("Read Resp: %s", err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("can't get adjacent nodes")
	}
	contents, err := ioutil.ReadAll(response.Body)
	defer response.Body.Close()
	if err != nil {
		return fmt.Errorf("failed to read response")
	}
	var nodes node.NodesType
	err = json.Unmarshal(contents, &nodes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal nodes")
	}

	if !reflect.DeepEqual(nodes, allNodes) {
		Trace.Printf("GET new ADJ Nodes\n")
		allNodes = nodes
		go allNodes.SaveNodes(filepath.Join(logDir, nodeFileName))
	}
	return nil
}

// getNodeID request the node id from the server.
// This is like the registration process.
func getNodeID() (uint64, error) {
	if myIP == "" {
		addresses, err := net.InterfaceAddrs()

		if err != nil {
			return 0, fmt.Errorf("can't get my own ip")
		}

		for _, a := range addresses {
			if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					myIP = ipnet.IP.String()
				}
			}
		}
	}
	Debug.Printf("My IP: %s\n", myIP)

	url := fmt.Sprintf("%v/node?ip=%s", myServer, myIP)
	Debug.Printf("PUT node: %v\n", url)

	request, err := http.NewRequest("PUT", url, nil)

	response, err := myClient.Do(request)

	if err != nil {
		return 0, fmt.Errorf("PUT node: %s", err)
	}

	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("can't get node id")
	}
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, fmt.Errorf("Read Resp: %s", err)
	}
	id, err := strconv.ParseUint(string(contents), 10, 64)
	Trace.Printf("PUT ID: %d\n", id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

func main() {
	debugPtr := flag.Bool("debug", false, "verbose output for debugging")
	serverPtr := flag.String("server", "haochen.net", "server url")
	portPtr := flag.Uint("port", 9999, "server listening port")
	p2pPortPtr := flag.Uint("myport", 8888, "p2p port")
	pubIPPtr := flag.String("pubip", "", "public IP address")
	logdirPtr := flag.String("logdir", "/tmp", "log file directory")

	flag.Parse()
	p2pPort = *p2pPortPtr
	myIP = *pubIPPtr
	logDir = *logdirPtr

	debugfile, err := os.OpenFile(filepath.Join(logDir, "c-debug.log"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		Error.Fatal(err)
	}
	defer debugfile.Close()

	tracefile, err := os.OpenFile(filepath.Join(logDir, "c.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		Error.Fatal(err)
	}
	defer tracefile.Close()

	if *debugPtr {
		tee := io.MultiWriter(debugfile, os.Stdout)
		initLogger(tee, tracefile, os.Stderr)
	} else {
		initLogger(debugfile, tracefile, os.Stderr)
	}

	myServer = fmt.Sprintf("http://%s:%d", *serverPtr, *portPtr)
	myClient = &http.Client{
		Timeout: 10 * time.Second,
	}

	Ledger = make(map[string]*recordT)
	MsgList = make(map[string]*msgRecord)

	// read nodes file for cached nodes info
	err = allNodes.ReadNodes(filepath.Join(logDir, nodeFileName))
	if err != nil {
		Error.Printf("read node file error: %v\n", err)
	}

	// get node id from server
	myID, err = getNodeID()
	if err != nil {
		Error.Fatal(err)
	} else {
		Debug.Printf("get myID: %d\n", myID)
	}

	// get adjacent node id every 30 sec from server
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	go func() {
		for t := range ticker.C {
			err = getAdjacentNodes()
			if err != nil {
				Error.Printf("get adjacent node error: %v/%v\n", err, t)
			}
		}
	}()

	http.HandleFunc("/data", dataHandler)

	s := http.Server{
		Addr:           fmt.Sprintf(":%d", *p2pPortPtr),
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	Error.Fatal(s.ListenAndServe())

}

// postData post the data to all the adjacent clients identified by id
// If the message had been sent to the clients already, it will skip.
func postData(data *P2PData, id string) error {
	Trace.Printf("P2P: %s,%s\n", id, data.FileName)

	n, ok := allNodes.Nodes[id]
	if !ok {
		Trace.Printf("P2P Node Not Found: %s,%s\n", id, data.FileName)
		return fmt.Errorf("can't find the node: %s", id)
	}

	url := fmt.Sprintf("http://%s:%d/data", n.IP, p2pPort)

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(*data)
	request, err := http.NewRequest("POST", url, b)
	request.Header.Add("Content-Type", "application/json")

	response, err := myClient.Do(request)

	if err != nil {
		Trace.Printf("P2P Post Failed: %s,%s,%s\n", id, data.FileName, err)
		return fmt.Errorf("POST data: %s", err)
	}

	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		Trace.Printf("P2P Post Status Not Okay: %s,%s\n", id, data.FileName)
		return fmt.Errorf("can't post data")
	}

	Trace.Printf("P2P DONE: %s,%s!\n", id, data.FileName)

	recordInLedger(data, id)

	return nil
}

// writeData will write the message data to file, input data is json encoded
func writeData(data *P2PData) error {
	dataFile, err := os.OpenFile(filepath.Join(logDir, data.FileName), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open data file error: %v", err)
	}
	defer dataFile.Close()

	dataFile.Write([]byte(data.Data))
	return nil
}

// foundInLedger will look for the transaction in Ledger
func foundInLedger(data *P2PData, id string) bool {
	if _, ok := Ledger[id]; ok {
		for _, msg := range Ledger[id].transaction {
			if strings.Compare(msg, data.FileName) == 0 {
				// found msg in the transaction
				return true
			}
		}
	}
	return false
}

// recordInLedger will record the transaction in Ledger
func recordInLedger(data *P2PData, id string) {
	if _, ok := Ledger[id]; ok {
		r := *Ledger[id]
		r.transaction = append(r.transaction, data.FileName)
		r.timestamp = append(r.timestamp, time.Now())
	} else {
		var record recordT
		record.transaction = make([]string, 0)
		record.timestamp = make([]time.Time, 0)
		record.transaction = append(record.transaction, data.FileName)
		record.timestamp = append(record.timestamp, time.Now())

		Ledger[id] = &record
	}
}

// notifyServer will send notification of received message to server
func notifyServer(data *P2PData) error {
	url := fmt.Sprintf("%s/data", myServer)

	notify := notification{Node: fmt.Sprintf("%d", myID), FileName: data.FileName, Timestamp: time.Now().String()}

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(&notify)
	request, err := http.NewRequest("POST", url, b)
	request.Header.Add("Content-Type", "application/json")

	response, err := myClient.Do(request)

	if err != nil {
		Error.Printf("notifying server failed: %s\n", data.FileName)
		return fmt.Errorf("POST data: %s", err)
	}

	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("can't post data")
	}

	Trace.Printf("NOTIFICATION: %s!\n", data.FileName)
	return nil
}

// check if node has received the same message
// If yes, we will skip write data and notify server
func receivedMsg(data *P2PData) bool {
	if _, ok := MsgList[data.FileName]; ok {
		return true
	}
	r := msgRecord{FileName: data.FileName, ShaOne: data.ShaOne, Timestamp: time.Now()}
	MsgList[data.FileName] = &r
	return false
}

// dataHandler handles POST /data request
// The request may come from peers or from external party as a trigger of messages
func dataHandler(w http.ResponseWriter, r *http.Request) {
	Trace.Printf("REQ /data: %v\n", r.RemoteAddr)
	if r.Method != "POST" {
		http.NotFound(w, r)
		return
	}

	if r.Body == nil {
		http.Error(w, "no data found in the POST request", http.StatusBadRequest)
		return
	}

	var data P2PData

	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		Error.Printf("Json decode failed %v\n", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if receivedMsg(&data) {
		io.WriteString(w, fmt.Sprintf("MSG received already!"))
		return
	}
	Trace.Printf("MSG: %s\n", data.FileName)

	go writeData(&data)

	// notify server about the message received
	// TODO: how to scale the notification?
	go notifyServer(&data)

	// try to find the transaction in ledger, if we have sent data other peers
	// we will skip, otherwise we use go routine to send data to all adjacent clients
	count := 0
	for id := range allNodes.Nodes {
		if foundInLedger(&data, id) {
			Trace.Printf("P2P SKIP: %s,%s\n", id, data.FileName)
		} else {
			go postData(&data, id)
			count = count + 1
		}
	}

	io.WriteString(w, fmt.Sprintf("DONE propagate %d nodes!", count))
}
