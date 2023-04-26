package nmsg

import (
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

func (p *NMSG) Request(ctx context.Context, channelID *string, sendData map[string]interface{}) (receiveData map[string]interface{}, err error) {
	MsgID := uuid.NewString()
	CreatedTime := time.Now()
	for key, value := range sendData {
		switch v := value.(type) {
		case []*multipart.FileHeader:
			result, err := p.uploadMultiPartFile(ctx, v, filepath.Join("tmp", MsgID, uuid.NewString()), nil, aws.Time(time.Now().Add(30*time.Minute)))
			if err != nil {
				return nil, err
			}
			sendData[key] = result
		case *multipart.FileHeader:
			result, err := p.uploadMultiPartFile(ctx, []*multipart.FileHeader{v}, filepath.Join("tmp", MsgID, uuid.NewString()), nil, aws.Time(time.Now().Add(30*time.Minute)))
			if err != nil {
				return nil, err
			}
			if len(result) == 1 {
				sendData[key] = result[0]
			} else {
				return nil, errors.New("some thing went wrong")
			}
		default:
		}

	}
	uniqueReplyTo := nats.NewInbox()

	// Listen for a single response
	sub, err := p.conn.SubscribeSync(uniqueReplyTo)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	deadline, ok := ctx.Deadline()
	expiredAt := &deadline
	if !ok {
		expiredAt = aws.Time(CreatedTime.Add(360 * time.Second))
	}

	netMsg := networkMessage{
		ExpiredAt: expiredAt,
		CreatedAt: &CreatedTime,
		Data:      sendData,
		ID:        uuid.NewString(),
	}

	// send to nats
	msgData, err := json.Marshal(netMsg)
	if err != nil {
		return nil, err
	}
	if err := p.conn.PublishRequest(*channelID, uniqueReplyTo, msgData); err != nil {
		return nil, err
	}
	p.conn.Flush()

	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		return nil, err
	}
	return result, nil

}

func (p *NMSG) QueueSubscribe(channelID *string, queue *string, fn func(ctx *Context)) (cancel func(), err error) {
	sub, err := p.conn.QueueSubscribe(*channelID, *queue, func(msg *nats.Msg) {
		msgData := networkMessage{}
		_ = json.Unmarshal(msg.Data, &msgData)

		ctx, cancel := context.WithDeadline(context.Background(), *msgData.ExpiredAt)
		defer cancel()
		ctx = context.WithValue(ctx, keySendMsg, msgData.Data)
		customContext := Context{
			Context: ctx,
		}
		//handle
		cmdDone := make(chan bool)
		go func() {
			fn(&customContext)
			msgRep := customContext.GetReplyMsg()
			if msgRep != nil {
				databytes, err := json.Marshal(msgRep)
				if err == nil {
					msg.Respond(databytes)
				}
				//do nothing
			}
			cmdDone <- true
		}()
		select {
		case <-ctx.Done():
			return
		case <-cmdDone:
			return
		}
	})

	if err != nil {
		return func() {
		}, err
	}

	return func() {
		_ = sub.Unsubscribe()
	}, nil

}
