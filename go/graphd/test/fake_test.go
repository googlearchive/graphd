package fakegraphd

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
)

const addr = ":8081"

var fg *FakeGraphd

func TestFakeReply(t *testing.T) {
	want := "ok ()\n"
	fg.SetReply(want)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Errorf("Error connecting to fakegraphd: %v", err)
	}
	fmt.Fprintf(conn, "status ()\n")
	r := bufio.NewReader(conn)
	got, err := r.ReadString('\n')
	if err != nil {
		t.Errorf("Unexpected error reading response: %v", err)
	}
	if got != want {
		t.Errorf("Unexpected reply after SetReply, got = %v, want = %v", got, want)
	}
}

func TestMain(m *testing.M) {
	fg = New(addr)
	cleanup, err := fg.Start()
	defer cleanup()
	if err != nil {
		log.Fatalf("Unexpected error starting fake graphd: %v", err)
	}
	os.Exit(m.Run())
}
