package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
)

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
func main() {
	bg := context.Background()
	cxt, cancel := context.WithCancel(bg)
	go server(cxt)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	cancel()
	fmt.Println("get signal from user:", s)
}
func server(cxt context.Context) {
	var sock = masterSock()
	err := os.Remove(sock)
	if err != nil {
		fmt.Println(err)
	}

	unixaddr, err := net.ResolveUnixAddr("unix", sock)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("get unixaddr from server:", unixaddr)
	listener, err := net.ListenUnix("unix", unixaddr)
	defer listener.Close()

	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("server is listening at:", unixaddr)
	for {
		select {
		case <-cxt.Done():
			fmt.Println("father cxt is done, and so this:", cxt)
		default:
			fmt.Println("father is not done")

		}
		c, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleRequest(c)
	}
}
func handleRequest(con net.Conn) {
	defer con.Close()

	buf := make([]byte, 128)
	nr, err := con.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = con.Write(buf[:nr])
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("content from conn:", con)
	fmt.Println(string(buf[:nr]))
}
