package wdb

import (
	"fmt"
	"testing"
)

var ws []*wrapperDB

func TestGet(t *testing.T) {
	w := &wrapperDB{dbuser:"test"}
	ws = append(ws, w)
	w.dbuser = "hello"
	fmt.Println("yoyo", ws[0].dbuser)
}
