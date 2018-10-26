package main

import (
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	"github.com/pricec/golib/log"
	message "github.com/pricec/protobuf/go/socket-gateway"
)

func main() {
	log.SetLevel(log.LL_DEBUG)
	defer log.Flush()

	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial("ws://kube-master:30808/open", nil)
	if err != nil {
		log.Err("Error connecting to server: %v", err)
		return
	}
	defer conn.Close()

	req := &message.EchoRequest{
		Version: uint32(1),
		Type: "echo",
		Id: 1,
		Data: "1234",
	}

	out, err := proto.Marshal(req)
	if err != nil {
		log.Err("Error marshaling request message: %v", err)
		return
	}

	if err := conn.WriteMessage(1, out); err != nil {
		log.Err("Failed to send message: %v", err)
	}
}
