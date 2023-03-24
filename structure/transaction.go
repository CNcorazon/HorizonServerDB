package structure

type (
	PubKeySign struct {
		// Pub string
		// Sign []byte
		R string
		S string
	}
	InternalTransaction struct {
		Shard uint
		From  string
		To    string
		Value int
		// Signature PubKeySign
	}

	CrossShardTransaction struct {
		Shard1 uint
		Shard2 uint
		From   string
		To     string
		Value  int
		// Signature PubKeySign
	}

	SuperTransaction struct {
		Shard uint
		To    string
		Value int
		// Signature PubKeySign
	}
)

func MakeInternalTransaction(s int, from string, to string, value int) Witnesstrans {
	trans := Witnesstrans{
		// Id:         id,
		Shard:      s,
		ToShard:    0,
		FromAddr:   from,
		ToAddr:     to,
		TransValue: value,
		TransType:  0,
		WitnessNum: 0,
	}
	return trans
}

func MakeCrossShardTransaction(s1 int, s2 int, from string, to string, value int) Witnesstrans {
	trans := Witnesstrans{
		// Id:         id,
		Shard:      s1,
		ToShard:    s2,
		FromAddr:   from,
		ToAddr:     to,
		TransValue: value,
		TransType:  1,
		WitnessNum: 0,
	}
	return trans
}
