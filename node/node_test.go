package node

import (
	"reflect"
	"testing"
)

func TestReadNodes(t *testing.T) {
	var tests = []struct {
		file string
		json NodesType
	}{
		{
			"test1.json",
			NodesType{
				Nodes: map[string]NodeJson{
					"1": NodeJson{Id: "1", IP: "192.168.2.1"},
					"2": NodeJson{Id: "2", IP: "192.168.2.2"},
				},
				MaxID: 3,
			},
		},
	}

	for _, test := range tests {
		var nodes NodesType
		err := nodes.ReadNodes(test.file)
		if err != nil {
			t.Errorf("ReadNodes Failed: %v\n", err)
		} else {
			if !reflect.DeepEqual(nodes, test.json) {
				t.Errorf("ReadNodes Unmarshal Failed.\nGot: %+v\nExpected: %+v\n", nodes, test.json)
			}
		}
	}
}
