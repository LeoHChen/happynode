//Package node abstract the representation of each node including server and client
//It provides read/save NodesType functions.
package node

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

// NodeJson is the internal representation of the p2p client
// each node is registered with server using a unique id
type NodeJson struct {
	Id string `json:"id"`
	IP string `json:"ip"`
}

// NodesType is structure of in-memory NodeJson DB
// Key is the Id of NodeJson
type NodesType struct {
	Nodes map[string]NodeJson `json:"nodes"`
	MaxID uint64              `json:"maxid"`
}

// SaveNodes marshals the NodesType to nodesFile on disk
// Input is the variable of NodesType and filename.
func (nodes *NodesType) SaveNodes(nodesFile string) error {
	file, err := os.OpenFile(nodesFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("SaveNodes: open file error: %v", err)
	}
	defer file.Close()

	bytes, err := json.Marshal(*nodes)
	if err != nil {
		return fmt.Errorf("SaveNodes: marshal error: %v", err)
	}
	_, err = file.Write(bytes)
	if err != nil {
		return fmt.Errorf("SaveNodes: write to file error: %v", err)
	}
	return nil
}

// ReadNodes unmarshals the nodesFile to in-memory NodesType structure
// Input is the filename. Output is a variable of NodesType.
func (nodes *NodesType) ReadNodes(nodesFile string) error {
	content, err := ioutil.ReadFile(nodesFile)
	if err != nil {
		return fmt.Errorf("unable to read %s", nodesFile)
	}

	err = json.Unmarshal([]byte(content), nodes)
	if err != nil {
		fmt.Errorf("ReadNodes: unmarshal error: %v", err)
		return err
	}
	return nil
}

// FindNode return the NodeJson if the given IP address match
func (nodes *NodesType) FindNode(ip string) *NodeJson {
	if nodes == nil || nodes.Nodes == nil {
		return nil
	}
	for _, v := range nodes.Nodes {
		if strings.Compare(v.IP, ip) == 0 {
			return &v
		}
	}
	return nil
}
