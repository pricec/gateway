package main

import (
	"flag"

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

	var url = flag.String("url", "https://www.google.com", "URL to GET")
	flag.Parse()

	req := &message.HttpRequest{
		Version: uint32(1),
		Type: message.RequestType_HTTP,
		Id: uint32(1),
		Url: *url,
	}

	out, err := proto.Marshal(req)
	if err != nil {
		log.Err("Error marshaling request message: %v", err)
		return
	}

	if err := conn.WriteMessage(1, out); err != nil {
		log.Err("Failed to send message: %v", err)
		return
	}

	_, p, err := conn.ReadMessage()
	if err != nil {
		log.Notice("Failed to read message: %v", err)
		return
	}

	resp := &message.HttpResponse{}
	if err := proto.Unmarshal(p, resp); err != nil {
		log.Err("Error unmarhsaling response: %v", err)
	} else {
		log.Notice("Received response %v", resp)
	}
}
