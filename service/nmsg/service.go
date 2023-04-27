package nmsg

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/mitchellh/mapstructure"
	"github.com/nats-io/nats.go"
)

const (
	keySendMsg  = iota
	keyReplyMsg = iota

	// ...
)

const (
	ServiceName = "nmsg" // Name of service.
	ServiceID   = "NMSG" // ServiceID is a unique identifier of a specific service.
)

type NmsgConfig struct {
	S3Sessions  *session.Session
	Bucket      *string
	NatsURL     *string
	NatsOptions []nats.Option
}

type NMSG struct {
	*NmsgConfig
	conn *nats.Conn
}
type networkMessage struct {
	ExpiredAt *time.Time
	Data      map[string]interface{}
	CreatedAt *time.Time
	ID        string
}

type Context struct {
	context.Context
}

func (p *Context) GetSendMsg() map[string]interface{} {
	result, _ := p.Context.Value(keySendMsg).(map[string]interface{})
	return result
}
func (p *Context) DecodeSendMessage(data interface{}) error {
	result, ok := p.Context.Value(keySendMsg).(map[string]interface{})
	if !ok {
		return errors.New("wrong data")
	}
	cfg := &mapstructure.DecoderConfig{
		Metadata: nil,
		Result:   data,
		TagName:  "json",
	}
	decoder, _ := mapstructure.NewDecoder(cfg)
	decoder.Decode(result)
	return nil
}

func (p *Context) GetReplyMsg() any {
	return p.Context.Value(keyReplyMsg)
}

func (p *Context) Reply(reply interface{}) {
	p.Context = context.WithValue(p.Context, keyReplyMsg, reply)
}

func New(config *NmsgConfig) (*NMSG, error) {
	conn, err := nats.Connect(*config.NatsURL, setupConnOptions(config.NatsOptions)...)
	if err != nil {
		return nil, err
	}

	return &NMSG{NmsgConfig: config,
		conn: conn}, nil
}
func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
	}))
	return opts
}
