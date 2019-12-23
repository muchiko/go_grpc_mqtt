package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/muchiko/go_grpc_mqtt/pb"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("127.0.0.1:50080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewSocketServiceClient(conn)
	ctx := context.Background()
	stream, err := client.Transport(ctx)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(resp.Message)
		}
	}()

	fmt.Println("Send.Message")
	err = stream.Send(&pb.Request{
		Message: "sub",
	})
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(10 * time.Second)
	defer stream.CloseSend()
}
