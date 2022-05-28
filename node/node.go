package node

type INode interface {
	GetAdders() string
	GetPort() int
	GetFullAddress() string
	GetType() string
}
