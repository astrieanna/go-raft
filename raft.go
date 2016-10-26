package main

import (
	"./RaftRPC"
	"fmt"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"os"
)

func main() {
	// 1 => localhost:2000
	// 2 => localhost:2001
	// 3 => localhost:2002

	my_ip := ":2000"

	my_addr, err := net.ResolveUDPAddr("udp", my_ip)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}

	buddy_addr, err := net.ResolveUDPAddr("udp", ":2001")
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}

	Connection, err := net.ListenUDP("udp", my_addr)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
	//defer Connection.Close()

	inbound := make(chan RaftRPC.RPCMessage)
	outbound := make(chan RaftRPC.RPCMessage)

	go func() {
		for {
			//func (c *UDPConn) WriteMsgUDP(b, oob []byte, addr *UDPAddr) (n, oobn int, err error)
			o := <-outbound
			data, err := proto.Marshal(&o)
			if err != nil {
				log.Fatal("marshaling error: ", err)
			}
			_, _, err = Connection.WriteMsgUDP(data, nil, buddy_addr)
			if err != nil {
				log.Fatal("error writing message: ", err)
			}
		}
	}()

	go func() {
		var b []byte;
		_, _, _, _, err := Connection.ReadMsgUDP(b, nil)
		//func (c *UDPConn) ReadMsgUDP(b, oob []byte) (n, oobn, flags int, addr *UDPAddr, err error)
		new_msg := RaftRPC.RPCMessage{}
		err = proto.Unmarshal(b, &new_msg)
        if err != nil {
            log.Fatal("unmarshaling error: ", err)
        }
		inbound <- new_msg
	}()

}
