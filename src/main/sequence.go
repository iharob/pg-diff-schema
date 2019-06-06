package main

type Sequence struct {
	name   string
	column *Column
	drops  bool
}

func (sequence Sequence) String() string {
	return sequence.name
}
