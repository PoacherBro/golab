package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type ClientConfig struct {
	Host   string
	Port   int32
	Path   string
	UseSSL bool
}

type Client struct {
	ctx        context.Context
	cancelCtx  context.CancelFunc
	wsConn     *websocket.Conn
	sendBuffer chan []byte
	wg         sync.WaitGroup
}

func NewClient(cfg *ClientConfig) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		ctx:        ctx,
		cancelCtx:  cancel,
		sendBuffer: make(chan []byte),
	}
	var err error
	c.wsConn, err = c.newWsConn(cfg)
	if err != nil {
		panic(err)
	}
	c.run()
	return c
}

func (c *Client) newWsConn(cfg *ClientConfig) (*websocket.Conn, error) {
	scheme := "ws"
	if cfg.UseSSL {
		scheme = "wss"
	}
	u := url.URL{Scheme: scheme, Host: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), Path: cfg.Path}
	conn, _, err := websocket.DefaultDialer.DialContext(c.ctx, u.String(), nil)
	if err != nil {
		logrus.Errorf("[wsClient] dial %s err: %v", u.String(), err)
		return nil, err
	}
	conn.SetPingHandler(func(appData string) error {
		logrus.Infof("[wsClient] ping: %s", appData)
		return nil
	})
	conn.SetPongHandler(func(appData string) error {
		logrus.Infof("[wsClient] pong: %s", appData)
		return nil
	})
	conn.SetCloseHandler(func(code int, text string) error {
		logrus.Infof("[wsClient] close handler: code=%d, text=%s", code, text)
		return nil
	})
	return conn, nil
}

func (c *Client) run() {
	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		c.sendLoop()
	}()
	go func() {
		defer c.wg.Done()
		c.readLoop()
	}()
}

func (c *Client) sendLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.sendBuffer:
			if !ok {
				return
			}
			logrus.Infof("[wsClient] ready to send message: %s", string(msg))
			err := c.wsConn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				logrus.Errorf("[wsClient] write message err: %v", err)
			}
		case <-ticker.C:
			c.wsConn.WriteMessage(websocket.PongMessage, []byte("ping"))
		}
	}
}

func (c *Client) readLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msgType, msg, err := c.wsConn.ReadMessage()
			if err != nil {
				logrus.Errorf("[wsClient] websocket read message err: %v", err)
				return
			}
			logrus.Infof("[wsClient] read message: type=%d, data=%s", msgType, string(msg))
		}
	}
}

func (c *Client) WriteMessage(msg []byte) error {
	select {
	case <-c.ctx.Done():
		return errors.New("send message to closed connection")
	default:
		select {
		case c.sendBuffer <- msg:
			return nil
		case <-time.After(3 * time.Second):
			return errors.New("send message timeout")
		}
	}
}

func (c *Client) Close() {
	c.cancelCtx()
	if err := c.wsConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		logrus.Errorf("[wsClient] write close message err: %v", err)
	}
	if err := c.wsConn.Close(); err != nil {
		logrus.Errorf("[wsClient] close websocket conn err: %v", err)
	}
	c.wg.Wait()
	close(c.sendBuffer)
}

func main() {
	cfg := &ClientConfig{
		Host:   "localhost",
		Port:   8080,
		Path:   "/ws",
		UseSSL: false,
	}
	client := NewClient(cfg)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case s := <-interrupt:
				logrus.Warnf("[main] receive signal: %s, will exist", s.String())
				return
			case t := <-ticker.C:
				err := client.WriteMessage([]byte(t.String()))
				if err != nil {
					logrus.Errorf("[main] client write message err: %v", err)
				}
			}
		}
	}()
	wg.Wait()
	client.Close()
}
