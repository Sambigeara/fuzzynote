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
	//url *url.URL
}

func NewWebsocketTarget(cfg webRemote) *WebsocketTarget {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	wsURI, err := url.Parse(cfg.WebsocketURL)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}
	conn, _, err := websocket.Dial(ctx, wsURI.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	//lambdaURL, err := url.Parse(cfg.URL)
	//if err != nil {
	//    // TODO fail silently - do not use websocket
	//    log.Fatal("dial:", err)
	//}

	return &WebsocketTarget{
		conn:          conn,
		mode:          cfg.Mode,
		pushMatchTerm: []rune(cfg.Match),
		//url:           lambdaURL,
	}
}

func (ws *WebsocketTarget) consume(walChan chan *[]EventLog) error {
	var err error
	// Might take a while for an event to come in, so timeouts aren't super useful here
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, message, err := ws.conn.Read(ctx)
	if err != nil {
		log.Println("read:", err)
	}
	strWal, _ := b64.StdEncoding.DecodeString(string(message))
	buf := bytes.NewBuffer([]byte(strWal))
	if el, err := BuildFromFile(buf); err == nil {
		walChan <- &el
	}
	return err
}

func (ws *WebsocketTarget) push(el EventLog) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	b := buildByteWal(&[]EventLog{el})
	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())
	err := ws.conn.Write(ctx, websocket.MessageText, []byte(b64Wal))
	if err != nil {
		log.Println("write:", err)
		return
	}
}
