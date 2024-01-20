package tsgen

import "fmt"

type nothing struct{}

const (
	// Instruction codes for all possible instructions.
	iRegNode = iota
	iConnect
	iPartition
	iWrite
	iRead
)

func Parse(input []byte) ([]Instr, error) {
	var result []Instr

	table := map[byte]func([]byte) (Instr, int, error){
		iRegNode:   parseRegNode,
		iConnect:   parseConnect,
		iPartition: parsePartition,
		iWrite:     parseWrite,
		iRead:      parseRead,
	}

	for len(input) > 0 {
		var node Instr
		parse, ok := table[input[0]]
		if !ok {
			return nil, fmt.Errorf("unknown instruction %d", input[0])
		}
		node, n, err := parse(input[1:])
		if err != nil {
			return nil, err
		}
		input = input[n:]
		result = append(result, node)
	}
	if err := validateProgram(result); err != nil {
		return nil, err
	}
	return result, nil
}

func parseRegNode(in []byte) (Instr, int, error) {
	if len(in) < 1 {
		return nil, 0, fmt.Errorf("missing node name for register node instr")
	}
	return RegisterNode{
		Node: nodeName(in[0]),
	}, 1, nil
}

func parseConnect(in []byte) (Instr, int, error) {
	if len(in) < 2 {
		return nil, 0, fmt.Errorf("missing two node names for connect instr")
	}
	return Connect{
		A: nodeName(in[0]),
		B: nodeName(in[1]),
	}, 2, nil
}

func parsePartition(in []byte) (Instr, int, error) {
	if len(in) < 2 {
		return nil, 0, fmt.Errorf("missing two node names for partition instr")
	}
	return Partition{
		A: nodeName(in[0]),
		B: nodeName(in[1]),
	}, 2, nil
}

func parseWrite(in []byte) (Instr, int, error) {
	if len(in) < 4 {
		return nil, 0, fmt.Errorf("missing four bytes for write instruction")
	}
	clientindex := in[0]
	if int(clientindex) >= len(clientNames) {
		return nil, 0, fmt.Errorf("cannot name client with %d, sorry", clientindex)
	}
	return Write{
		Client: clientNames[clientindex],
		Node:   nodeName(in[1]),
		Key:    fmt.Sprintf("%02x", in[2]),
		Value:  fmt.Sprintf("%02x", in[3]),
	}, 4, nil
}

func parseRead(in []byte) (Instr, int, error) {
	if len(in) < 3 {
		return nil, 0, fmt.Errorf("missing four bytes for read instruction")
	}
	clientindex := in[0]
	if int(clientindex) >= len(clientNames) {
		return nil, 0, fmt.Errorf("cannot name client with %d, sorry", clientindex)
	}
	return Write{
		Client: clientNames[clientindex],
		Node:   nodeName(in[1]),
		Key:    fmt.Sprintf("%02x", in[2]),
	}, 3, nil
}

func nodeName(b byte) string {
	return fmt.Sprintf("node_%02x", b)
}

func validateProgram(program []Instr) error {
	nodes := map[string]nothing{}
	for idx, instr := range program {
		switch v := instr.(type) {
		case RegisterNode:
			nodes[v.Node] = nothing{}
		case Read:
			if _, ok := nodes[v.Node]; !ok {
				return fmt.Errorf("instruction %d: node does not exist", idx)
			}
		case Write:
			if _, ok := nodes[v.Node]; !ok {
				return fmt.Errorf("instruction %d: node does not exist", idx)
			}
		case Connect:
			if _, ok := nodes[v.A]; !ok {
				return fmt.Errorf("instruction %d: node does not exist", idx)
			}
			if _, ok := nodes[v.B]; !ok {
				return fmt.Errorf("instruction %d: node does not exist", idx)
			}
		case Partition:
			if _, ok := nodes[v.A]; !ok {
				return fmt.Errorf("instruction %d: node does not exist", idx)
			}
			if _, ok := nodes[v.B]; !ok {
				return fmt.Errorf("instruction %d: node does not exist", idx)
			}
		}
	}
	return nil
}
