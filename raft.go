package main

import (
	"flag"
	"fmt"
	"github.com/astrieanna/go-raft/RaftRPC"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"os"
)

type ServerConfig struct {
	id               uint64
	servers          map[uint64]string
	election_timeout uint64 // milliseconds
}

type PersistentState struct {
	currentTerm uint64
	votedFor    uint64
	log         uint64
}

type VolatileState struct {
	commitIndex uint64
	lastApplied uint64
}

type LeaderVolatileState struct {
	nextIndex  []uint64
	matchIndex []uint64
}

type Mode int

const (
	FOLLOWER Mode = iota
	LEADER
	CANDIDATE
)

type ServerState struct {
	mode     Mode
	config   ServerConfig
	pstate   PersistentState
	vstate   VolatileState
	inbound  chan RaftRPC.RPCMessage
	outbound chan RaftRPC.RPCMessage
}

func follower_handler(state ServerState) {
	fmt.Println("I am a follower with id ", state.config.id)

	for {
		m := <-state.inbound
		fmt.Println("\tI received a message")

		aereq := m.GetAereq()
		if aereq != nil {
			fmt.Println("\tGot an AE request")
			continue
		}
		aereply := m.GetAereply()
		if aereply != nil {
			fmt.Println("\tGot an AE reply")
			continue
		}

		// if timer fires, set mode to candidate and return
	}
}

func leader_handler(state ServerState, lvstate LeaderVolatileState) {
	fmt.Println("I am the leader with id ", state.config.id)
	le := RaftRPC.LogEntry{Index: 1, Term: 1, Key: "x", Value: []byte("hello")}
	mreq := RaftRPC.AppendEntriesRequest{
		Term:         1,
		LeaderId:     state.config.id, // me! I'm the leader!
		PrevLogIndex: 0,               // first entry
		PrevLogTerm:  0,               // before my time
		Entry:        []*RaftRPC.LogEntry{&le},
		LeaderCommit: 0,
	}

	buddy_id := uint64(2)
	rpc := RaftRPC.RPCMessage{ServerId: buddy_id, Message: &RaftRPC.RPCMessage_Aereq{&mreq}}

	state.outbound <- rpc
	fmt.Println("I sent a message to ", buddy_id)

	// read incoming messages
	// if receive msg from new leader, set mode to follower and return
	// if timer fires, send heart beats
}

func candidate_handler(state ServerState) {
	fmt.Println("I am a candidate")
	// send requests for votes
	// if receive message from new leader, set mode to follower and return
	// if receive enough votes, set mode to leader and return
	// if timer fires, return (start new election)
}

func main() {
	amLeader := flag.Bool("leader", false, "if set, this node will start as leader")
	idPtr := flag.Int("id", 0, "this server's id")
	flag.Parse()

	config := ServerConfig{uint64(*idPtr),
		map[uint64]string{
			1: "localhost:2000",
			2: "localhost:2001",
			3: "localhost:2002",
		},
		5000,
	}
	my_addr, err := net.ResolveUDPAddr("udp", config.servers[config.id])
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}

	connection, err := net.ListenUDP("udp", my_addr)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
	//defer connection.Close()

	fmt.Println("Listening on ", my_addr)

	// Go routine for sending messages; just stick them in the channel
	outbound := make(chan RaftRPC.RPCMessage)
	go func() {
		for {
			o := <-outbound
			fmt.Println("Sending a Message!")
			sid := o.ServerId
			o.ServerId = config.id
			data, err := proto.Marshal(&o)
			if err != nil {
				log.Fatal("marshaling error: ", err)
			}
			switch sid {
			case config.id:
				log.Fatal("Asked to send a message to myself...")
			case 0:
				for k, v := range config.servers {
					if k != config.id {
						buddy_addr, err := net.ResolveUDPAddr("udp", v)
						if err != nil {
							log.Fatal("error resolving udpaddr", v, ": ", err)
						}
						_, _, err = connection.WriteMsgUDP(data, nil, buddy_addr)
						if err != nil {
							log.Fatal("error writing message: ", err)
						}
					}
				}
			default:
				buddy_addr, err := net.ResolveUDPAddr("udp", config.servers[sid])
				fmt.Println("Sending a message to ", sid, " at ", buddy_addr)
				if err != nil {
					log.Fatal("error resolving udpaddr", config.servers[sid], ": ", err)
				}
				_, _, err = connection.WriteMsgUDP(data, nil, buddy_addr)
				if err != nil {
					log.Fatal("error writing message: ", err)
				}

			}
		}
	}()

	// Go routine for receivng messages; will stick them in the channel
	inbound := make(chan RaftRPC.RPCMessage)
	go func() {
		var b []byte
		for {
			n, _, _, _, err := connection.ReadMsgUDP(b, nil)
			if err != nil {
				log.Fatal("read msg error: ", err)
			}
			if n > 0 {
				fmt.Println("Received a message!")
				new_msg := RaftRPC.RPCMessage{}
				err = proto.Unmarshal(b, &new_msg)
				if err != nil {
					log.Fatal("unmarshaling error: ", err)
				}
				inbound <- new_msg
			}
		}
	}()

	// Timer routine; can set or reset via channel
	//timer_command_channel
	//timer_ping_channel
	//	go func() {
	// how do I timer?
	//	}()

	// Handle message properly according to current mode
	state := ServerState{
		mode:     FOLLOWER,
		config:   config,
		inbound:  inbound,
		outbound: outbound,
	}
	if *amLeader {
		state.mode = LEADER
	}
	lvstate := LeaderVolatileState{}
	for {
		switch state.mode {
		case FOLLOWER:
			follower_handler(state)
		case LEADER:
			leader_handler(state, lvstate)
		case CANDIDATE:
			candidate_handler(state)
		}
	}

}
