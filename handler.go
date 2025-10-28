package protovnats

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
)

type NatsHandler struct {
	Report              func(error)
	Conn                *nats.Conn
	Subscriptions       []func(*nats.Conn) (*nats.Subscription, error)
	ActiveSubscriptions map[string]*nats.Subscription
	mut                 sync.Mutex
}

func New(report func(error)) *NatsHandler {
	out := new(NatsHandler)
	out.Subscriptions = make([]func(*nats.Conn) (*nats.Subscription, error), 0)
	out.Report = report
	out.ActiveSubscriptions = make(map[string]*nats.Subscription)
	return out
}

func (n *NatsHandler) Start(url string, opts ...nats.Option) error {
	n.mut.Lock()
	if n.Conn != nil {
		n.mut.Unlock()
		return errors.New("already started")
	}
	conn, err := nats.Connect(url, opts...)
	if err != nil {
		n.mut.Unlock()
		return err
	}
	n.Conn = conn
	for _, subscription := range n.Subscriptions {
		out, err := subscription(n.Conn)
		if err != nil {
			n.mut.Unlock()
			return errors.Join(err, n.Stop())
		}
		n.ActiveSubscriptions[out.Subject] = out
	}
	n.mut.Unlock()
	return nil
}

func (n *NatsHandler) Stop() error {
	n.mut.Lock()
	defer n.mut.Unlock()
	if n.Conn == nil {
		return errors.New("not started")
	}
	for key, subscription := range n.ActiveSubscriptions {
		if err := subscription.Drain(); err != nil {
			return err
		}
		delete(n.ActiveSubscriptions, key)
	}
	if err := n.Conn.Drain(); err != nil {
		return err
	}
	n.Conn = nil
	return nil
}

func (n *NatsHandler) Handle(subject string, queue string, fn func(context.Context, []byte) ([]byte, error)) error {
	n.mut.Lock()
	defer n.mut.Unlock()
	if n.Conn != nil {
		return errors.New("already started")
	}
	n.Subscriptions = append(n.Subscriptions, func(conn *nats.Conn) (*nats.Subscription, error) {
		return conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
			out, err := fn(context.TODO(), msg.Data)
			if err != nil {
				if n.Report != nil {
					n.Report(fmt.Errorf("subject: %s, queue: %s, error: %w", subject, queue, err))
				}
				return
			}
			if err := msg.Respond(out); err != nil {
				if n.Report != nil {
					n.Report(fmt.Errorf("subject: %s, queue: %s, error: %w", subject, queue, err))
				}
			}
		})
	})
	return nil
}
