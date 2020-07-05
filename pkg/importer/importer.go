package importer

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

const (
	fileName  = "pkg/importer/workflowy_test.xml"
	nodeTitle = "outline"
)

type Time struct {
	XMLName xml.Name `xml:"time"`
	Year    xml.Attr `xml:"startYear,attr"`
	Month   xml.Attr `xml:"startMonth,attr"`
	Day     xml.Attr `xml:"startDay,attr"`
	Content []byte   `xml:",innerxml"`
}

type Node struct {
	XMLName xml.Name
	//XMLName xml.Name   `xml:"outline"`
	Text  xml.Attr   `xml:"text,attr"`
	Note  xml.Attr   `xml:"note,attr"`
	Nodes []Node     `xml:",any"`
	Attrs []xml.Attr `xml:"-"`
	//Time  Time
	//Content []byte     `xml:",innerxml"`
}

func (n *Node) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	n.Attrs = start.Attr
	type node Node

	return d.DecodeElement((*node)(n), &start)
}

func walk(nodes []Node, chain []string, f func(Node) bool) {
	for _, n := range nodes {
		newChain := chain
		if f(n) {
			newChain = append(newChain, n.Text.Value)
		}
		walk(n.Nodes, newChain, f)
		fullString := strings.Join(newChain, " >> ")
		fmt.Printf("%v\n", fullString)
	}
}

func ImportLines() error {
	dat, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal(err)
		return err
	}

	buf := bytes.NewBuffer(dat)
	dec := xml.NewDecoder(buf)

	var n Node
	err = dec.Decode(&n)
	if err != nil {
		panic(err)
	}

	walk([]Node{n}, []string{}, func(n Node) bool {
		if n.XMLName.Local == nodeTitle {
			//fmt.Printf("%v, %v\n", n.Text.Value, n.Note.Value)
			return true
		}
		return false
	})
	return nil
}
