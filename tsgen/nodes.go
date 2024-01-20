package tsgen

type RegisterNode struct {
	Node string
}

func (rn RegisterNode) Apply(m Model, i Impl) error {
	m.Add(rn.Node)
	return i.CreateNode(rn.Node)
}

type Write struct {
	Client     string
	Node       string
	Key, Value string
}

func (w Write) Apply(m Model, i Impl) error {
	return i.Write(w.Client, w.Node, w.Key, w.Value)
}

type Read struct {
	Client string
	Node   string
	Key    string
}

func (r Read) Apply(m Model, i Impl) error {
	return i.Read(r.Client, r.Node, r.Key)
}

type Connect struct {
	A, B string
}

func (c Connect) Apply(m Model, i Impl) error {
	m.Connect(c.A, c.B)
	return nil
}

type Partition struct {
	A, B string
}

func (p Partition) Apply(m Model, i Impl) error {
	m.Partition(p.A, p.B)
	return nil
}
