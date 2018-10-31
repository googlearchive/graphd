// package fakegraphd implements a server used for testing graphd clients.
// This package will start a TCP server on a specified port, and are able to behave
// deterministically (and for testing, pathologically) with respect to configuration.
package fakegraphd

import (
	"fmt"
	"log"
	"net"
	"sync"
)

type FakeGraphd struct {
	Addr      string
	Reply     string
	replyLock sync.RWMutex
}

func New(addr string) *FakeGraphd {
	return &FakeGraphd{Addr: addr}
}

func (f *FakeGraphd) SetReply(reply string) {
	f.replyLock.Lock()
	defer f.replyLock.Unlock()
	f.Reply = reply
}

func (f *FakeGraphd) Start() (func() error, error) {
	ln, err := net.Listen("tcp", f.Addr)
	if err != nil {
		return nil, fmt.Errorf("Error starting graphd: %v", err)
	}
	log.Printf("Listening: %v", ln.Addr())
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("UH OH: %v", err)
			}
			log.Printf("Handle: %v", conn)
			go f.handle(conn)
		}
	}()
	return ln.Close, nil
}

func (f *FakeGraphd) handle(c net.Conn) {
	f.replyLock.RLock()
	defer f.replyLock.RUnlock()
	fmt.Fprintf(c, f.Reply)
}
