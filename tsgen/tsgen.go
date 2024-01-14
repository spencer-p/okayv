package tsgen

type Program []Instr

type Instr interface {
	Apply(Model, Impl) error
}

type Impl interface {
	CreateNode(name string) error
	Read(client, node, key string) error
	Write(client, node, key, value string) error
}

type Model struct {
	Nodes      map[string]struct{}
	Partitions map[string]map[string]struct{}
}

func NewModel() Model {
	return Model{
		Nodes:      make(map[string]struct{}),
		Partitions: make(map[string]map[string]struct{}),
	}
}

func (m Model) Add(node string) {
	m.Nodes[node] = struct{}{}
}

func (m Model) Remove(node string) {
	delete(m.Nodes, node)
}

func (m Model) Partition(a, b string) {
	m.partition(a, b)
	m.partition(b, a)
}

func (m Model) partition(a, b string) {
	dropped := m.Partitions[a]
	if dropped == nil {
		dropped = make(map[string]struct{})
	}
	dropped[b] = struct{}{}
	m.Partitions[a] = dropped
}

func (m Model) Connect(a, b string) {
	m.connect(a, b)
	m.connect(b, a)
}

func (m Model) connect(a, b string) {
	dropped := m.Partitions[a]
	if dropped == nil {
		return
	}
	delete(dropped, b)
}

// Reachable returns true if nodes a and b exist and there is a network path for
// them to reach one another. The path does not need to be direct.
func (m Model) Reachable(a, b string) bool {
	queue := []string{a}
	queued := map[string]struct{}{
		a: {},
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		if cur == b {
			return true
		}

		dropped := m.Partitions[cur]
		for n := range m.Nodes {
			if _, skip := queued[n]; skip {
				continue
			}
			if _, drop := dropped[n]; drop {
				continue
			}
			queue = append(queue, n)
			queued[n] = struct{}{}
		}
	}

	return false
}
