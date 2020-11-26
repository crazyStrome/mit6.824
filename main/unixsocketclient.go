package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
func main() {
	addr, err := net.ResolveUnixAddr("unix", masterSock())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("client is conneting to", addr)
	con, err := net.DialUnix("unix", nil, addr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer con.Close()
	_, err = con.Write([]byte("hello world"))
	if err != nil {
		fmt.Println(err)
		return
	}
	buf := make([]byte, 128)
	nr, err := con.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("content from server:", string(buf[:nr]))
}
