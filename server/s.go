//Package server is the program runs on the server node
//It is responsible for assigning node id to clients
//and send the list of adjacent nodes to clients as well.
//
//APIs supported by the server
// GET  /node    return the list of adjacent nodes, including node ids and ip addresses
// PUT  /node    return a node id to client, register the client's ip address in node db
// POST /data    keep track of when/what messages received by clients
package main

import (
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
	"strings"
	"time"
)

import (
	"github.com/LeoHChen/happynode/node"
)

type link struct {
	s string
	e string
}

type notification struct {
	Node      string `json:"node"`
	FileName  string `json:"file"`
	Timestamp string `json:"ts"`
}

// implement the String interface for printing
func (n notification) String() string {
	return fmt.Sprintf(`{ "node":"%s", "file":"%s", "ts":"%s" }`, n.Node, n.FileName, n.Timestamp)
}

const (
	edgeFileName = "edges.txt"
	nodeFileName = "nodes.json"
)

var (
	Debug *log.Logger
	Trace *log.Logger
	Error *log.Logger

	// in memory representation of all nodes
	allNodes node.NodesType

	// key is the id of node, value is list of adjacent nodes
	LinkMap map[string][]string

	nodeFile *os.File
	logDir   string

	myIP string
)

func initLogger(debugLog io.Writer, traceLog io.Writer, errorLog io.Writer) {
	Debug = log.New(debugLog, "DEBUG: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	Trace = log.New(traceLog, "TRACE: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	Error = log.New(errorLog, "ERROR: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
}

func main() {
	debugPtr := flag.Bool("debug", false, "verbose output for debugging")
	port := flag.Uint("port", 9999, "server listening port")
	ipPtr := flag.String("pubip", "", "server listening pub ip address")
	logDirPtr := flag.String("logdir", "/tmp", "log file directory")
	flag.Parse()
	logDir = *logDirPtr
	myIP = *ipPtr

	debugfile, err := os.OpenFile(filepath.Join(logDir, "s-debug.log"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		Error.Fatal(err)
	}
	defer debugfile.Close()

	tracefile, err := os.OpenFile(filepath.Join(logDir, "s.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
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

	// read nodes file for cached nodes info
	err = allNodes.ReadNodes(filepath.Join(logDir, nodeFileName))
	if err != nil {
		Error.Printf("read node file error: %v\n", err)
	}

	err = readEdges()

	if err != nil {
		Error.Fatal(err)
	}

	// save node file every 300 sec
	// TODO: save only when allNodes changed
	ticker := time.NewTicker(300 * time.Second)
	defer ticker.Stop()

	go func() {
		for t := range ticker.C {
			err := allNodes.SaveNodes(filepath.Join(logDir, nodeFileName))
			if err != nil {
				Error.Printf("Save Nodes error: %v/%v\n", err, t)
			}
		}
	}()

	Trace.Printf("myIP: %s, logDir: %s, port: %d\n", myIP, logDir, *port)

	http.HandleFunc("/node", nodeHandler)
	http.HandleFunc("/data", dataHandler)

	s := http.Server{
		Addr:           fmt.Sprintf("%s:%d", myIP, *port),
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	Error.Fatal(s.ListenAndServe())
}

// function to handle the /data request
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

	var notify notification

	err := json.NewDecoder(r.Body).Decode(&notify)
	if err != nil {
		Error.Printf("Json decode failed %v\n", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	Trace.Printf("NOTIFICATION: %s\n", notify)
	io.WriteString(w, http.StatusText(http.StatusOK))
}

// function to handle the /node request
func nodeHandler(w http.ResponseWriter, r *http.Request) {
	var res string
	q := r.URL.Query()

	if r.Method == "GET" {
		if id, ok := q["id"]; ok {
			n, ok := allNodes.Nodes[id[0]]
			if !ok {
				Error.Printf("can't find node: %s\n", id[0])
				http.NotFound(w, r)
				return
			}
			var adjNodes node.NodesType
			adjNodes.Nodes = make(map[string]node.NodeJson)
			if nl, ok := LinkMap[n.Id]; ok {
				for _, an := range nl {
					if thenode, ok := allNodes.Nodes[an]; ok {
						adjNodes.Nodes[an] = thenode
					}
				}
				bytes, err := json.Marshal(adjNodes)
				if err != nil {
					Error.Printf("can't marshal node: %s\n", id[0])
					http.NotFound(w, r)
					return
				}
				res = string(bytes)
			} else {
				res = ""
			}
		} else {
			Error.Println("no id found in GET")
			http.NotFound(w, r)
			return
		}
	}

	if r.Method == "PUT" {
		if ip, ok := q["ip"]; ok {
			oneip := net.ParseIP(ip[0])
			if oneip == nil {
				Error.Printf("invalid ip address: %s\n", ip[0])
				http.NotFound(w, r)
				return
			}
			if onenode := allNodes.FindNode(ip[0]); onenode == nil {
				// add new node
				var n node.NodeJson
				n.Id = fmt.Sprintf("%d", allNodes.MaxID)
				n.IP = ip[0]
				if allNodes.Nodes == nil {
					allNodes.Nodes = make(map[string]node.NodeJson)
				}
				allNodes.Nodes[n.Id] = n

				Trace.Printf("PUT new node: %v [%v]\n", allNodes.MaxID, oneip)
				allNodes.MaxID = allNodes.MaxID + 1
				res = n.Id
			} else {
				res = onenode.Id
			}
		} else {
			Error.Println("no ip found in PUT")
			http.NotFound(w, r)
			return
		}
	}
	io.WriteString(w, res)
}

//readEdges parse the edgeFile and keep the edge info in the Link/LinkMap DB
func readEdges() error {
	Links := make([]link, 0)
	LinkMap = make(map[string][]string)

	content, err := ioutil.ReadFile(edgeFileName)
	if err != nil {
		return fmt.Errorf("unable to read %s", edgeFileName)
	}

	lines := strings.Split(string(content), "\n")

	for _, lines := range lines {
		fields := strings.Fields(lines)
		if len(fields) != 2 {
			continue
		}

		Links = append(Links, link{fields[0], fields[1]})
	}

	for _, l := range Links {
		if _, ok := LinkMap[l.s]; ok {
			LinkMap[l.s] = append(LinkMap[l.s], l.e)
		} else {
			LinkMap[l.s] = make([]string, 0)
			LinkMap[l.s] = append(LinkMap[l.s], l.e)
		}
	}
	return nil
}
