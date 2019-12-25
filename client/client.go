package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/muchiko/go_grpc_chat/pb"
	"google.golang.org/grpc"
)

func Run() {
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

	go receive(stream)

	err = send(stream)
	if err != nil {
		log.Fatal(err)
	}
}

func send(stream pb.SocketService_TransportClient) error {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		if scanner.Scan() {
			text := scanner.Text()
			err := stream.Send(&pb.Request{
				Message: text,
			})
			if err != nil {
				stream.CloseSend()
				return err
			}
		}
	}
}

func receive(stream pb.SocketService_TransportClient) {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}
		fmt.Println(resp.Message)
	}
}
