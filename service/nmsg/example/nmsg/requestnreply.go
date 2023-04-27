package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/swop-hcmc/swop-sdk/service/nmsg"
)

func main() {
	session, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-1"),
		Credentials: credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY"), os.Getenv("AWS_SECRET_KEY"), ""),
	})
	if err != nil {
		panic(err)
	}
	client, err := nmsg.New(&nmsg.NmsgConfig{
		Bucket:     aws.String(os.Getenv("BUCKET")),
		NatsURL:    aws.String(os.Getenv("NATS_URL")),
		S3Sessions: session,
	})
	if err != nil {
		panic(err)
	}
	//add listerner
	go queueSub(client)
	dataToSend := map[string]interface{}{
		"request": "value1",
	}
	ctxWTo, _ := context.WithTimeout(context.Background(), 1*time.Second)
	rec, err := client.Request(ctxWTo, aws.String("channel_test"), dataToSend)
	if err != nil {
		log.Println("request_err: ", err)
	} else {
		log.Println("Replied Msg: ", rec)
	}
}
func queueSub(c *nmsg.NMSG) {
	cancel, err := c.QueueSubscribe(aws.String("channel_test"), aws.String("abc"), func(ctx *nmsg.Context) {
		//Uncomment to see the request error deadline exceeded
		//time.Sleep(5 * time.Second)
		log.Println("Received Msg: ", ctx.GetSendMsg())
		//decode using json
		type SampleStruct struct {
			Request string `json:"request"`
		}
		a := SampleStruct{}
		ctx.DecodeSendMessage(&a)
		log.Println("Received struct messages: ", a)

		ctx.Reply(
			map[string]interface{}{
				"rep1": "value1",
			},
		)
	})
	if err != nil {
		log.Println(err)
	}
	defer cancel()
	time.Sleep(500 * time.Second)
	//log.Println(rec)

}
