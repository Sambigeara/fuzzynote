package service

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"log"
	"net/url"
	"time"

	"nhooyr.io/websocket"
)

type WebsocketTarget struct {
	conn          *websocket.Conn
	mode          Mode
	pushMatchTerm []rune
	//url           url.URL
}

func NewWebsocketTarget(cfg websocketRemote) *WebsocketTarget {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	url, err := url.Parse(cfg.URLString)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}
	conn, _, err := websocket.Dial(ctx, url.String(), nil)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("dial:", err)
	}
	return &WebsocketTarget{
		conn:          conn,
		mode:          cfg.Mode,
		pushMatchTerm: []rune(cfg.Match),
		//url:           cfg.URL,
	}
}

func (ws *WebsocketTarget) consume(walChan chan *[]EventLog) error {
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, message, err := ws.conn.Read(ctx)
	if err != nil {
		log.Println("read:", err)
	}
	strWal, _ := b64.StdEncoding.DecodeString(string(message))
	buf := bytes.NewBuffer([]byte(strWal))
	if el, err := buildFromFile(buf); err == nil {
		walChan <- &el
	}
	return err
}

func (ws *WebsocketTarget) push(el EventLog) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b := buildByteWal(&[]EventLog{el})
	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())
	err := ws.conn.Write(ctx, websocket.MessageText, []byte(b64Wal))
	if err != nil {
		log.Println("write:", err)
		return
	}
}
