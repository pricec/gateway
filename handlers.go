package main

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pricec/golib/kafka"
	"github.com/pricec/golib/log"
	"github.com/pricec/gateway/session"
	message "github.com/pricec/protobuf/go/socket-gateway"
)

func responseCb(s *session.SessionManager) func([]byte) {
	return func(data []byte) {
		resp := &message.Response{}
		if err := proto.Unmarshal(data, resp); err != nil {
			log.Notice("Received bad response message '%v': %v", data, err)
			return
		} else {
			log.Debug("Received response message: %+v", resp)
		}

		idStr := resp.GetClientId()
		if id, err := session.NewSessionId(idStr); err != nil {
			log.Notice(
				"Received message for nonexistent client '%v': %v",
				idStr,
				err,
			)
			return
		} else {
			s.Write(id, data)
		}
	}
}

func decodeRequest(data []byte, id session.SessionId) (proto.Message, error) {
	log.Debug("Bytes received: %+v", data)
	log.Debug("String reveived: %v", string(data))

	req := &message.Request{}
	if err := proto.Unmarshal(data, req); err != nil {
		return nil, fmt.Errorf("Error unmarshaling request: %v", err)
	}

	reqType := req.GetType()
	switch reqType {
	case message.RequestType_ECHO:
		request := &message.EchoRequest{}
		if err := proto.Unmarshal(data, request); err != nil {
			return nil, fmt.Errorf("Error unmarshaling request: %v", err)
		}
		request.ClientId = id.String()
		log.Debug("Unmarshaled request (with clientId): %v", request)
		return request, nil
	case message.RequestType_HTTP:
		request := &message.HttpRequest{}
		if err := proto.Unmarshal(data, request); err != nil {
			return nil, fmt.Errorf("Error unmarshaling request: %v", err)
		}
		request.ClientId = id.String()
		log.Debug("Unmarshaled request (with clientId): %v", request)
		return request, nil
	default:
		return nil, fmt.Errorf("Unrecognized request type %v", reqType)
	}
}

func requestCb(
	km *kafka.KafkaManager,
	requestTopic string,
) func(*session.SessionManager, session.SessionId, []byte) {
	return func(s *session.SessionManager, id session.SessionId, data []byte) {
		req, err := decodeRequest(data, id)
		if err != nil {
			log.Err("Failed to decode request for %v: %v", id, err)
		} else {
			if out, err := proto.Marshal(req); err != nil {
				log.Err("Failed to marshal modified request: %v", err)
			} else if err := km.Send(requestTopic, out); err != nil {
				log.Err("Failed to send message '%v' to kafka: %v", data, err)
				// TODO: send a message indicating the failure
			} else {
				log.Debug("Sent message %+v (bytes %v)", req, out)
			}
		}
	}
}
