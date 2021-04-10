package service

import (
	"bytes"
	b64 "encoding/base64"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
)

type WebsocketTarget struct {
	conn          *websocket.Conn
	mode          Mode
	pushMatchTerm []rune
	//url           url.URL
}

func NewWebsocketTarget(cfg websocketRemote) *WebsocketTarget {
	url, err := url.Parse(cfg.URLString)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}
	conn, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
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

func (ws *WebsocketTarget) consume(walChan chan *[]EventLog) {
	go func() {
		for {
			_, message, err := ws.conn.ReadMessage()
			if err != nil {
				log.Println("read:", err)
			}
			// TODO decode from b64??
			strWal, _ := b64.StdEncoding.DecodeString(string(message))
			buf := bytes.NewBuffer([]byte(strWal))
			if el, err := buildFromFile(buf); err == nil {
				walChan <- &el
			}
		}
	}()
}

func (ws *WebsocketTarget) push(el EventLog) {
	// TODO encode to b64??
	// for ref
	//strWal := b64.StdEncoding.EncodeToString(b)
	//b, _ = b64.StdEncoding.DecodeString(strWal)
	//el = getMatchedWal(el, wf)
	b := BuildByteWal(&[]EventLog{el})
	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())
	err := ws.conn.WriteMessage(websocket.TextMessage, []byte(b64Wal))
	if err != nil {
		log.Println("write:", err)
		return
	}
}
