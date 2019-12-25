package server

import (
	"fmt"
	"io"
	"log"
	"net"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/muchiko/go_grpc_chat/pb"
	"google.golang.org/grpc"
)

func Run() {
	lis, err := net.Listen("tcp", ":50080")
	if err != nil {
		log.Fatal(err)
	}
	defer lis.Close()

	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://0.0.0.0:1883")
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	s := grpc.NewServer()
	pb.RegisterSocketServiceServer(s, &server{
		mqdb: &client,
	})
	err = s.Serve(lis)
	if err != nil {
		log.Fatal(err)
	}
}

type server struct {
	mqdb *mqtt.Client
}

func (s *server) Transport(stream pb.SocketService_TransportServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		fmt.Println(in.Message)

		// call
		switch in.Message {
		case "sub":
			topic := "room/#"
			qos := byte(1)
			token := (*s.mqdb).Subscribe(topic, qos, func(client mqtt.Client, msg mqtt.Message) {
				fmt.Println("Transport")
				stream.Send(&pb.Payload{
					Message: string(msg.Payload()),
				})
			})
			if token.Wait() && token.Error() != nil {
				return token.Error()
			}
		case "pub":
			topic := "room"
			qos := byte(1)
			retained := false
			token := (*s.mqdb).Publish(topic, qos, retained, "in-api")
			if token.Wait() && token.Error() != nil {
				return token.Error()
			}
		default:
			return nil
		}

		err = stream.Send(&pb.Payload{
			Message: "OK",
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}
