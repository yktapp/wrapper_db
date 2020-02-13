package wdb

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"testing"
)

var ws []*WrapperDB

func TestGet(t *testing.T) {
	w := &WrapperDB{dbuser: "test"}
	ws = append(ws, w)
	w.dbuser = "hello"
	fmt.Println("yoyo", ws[0].dbuser)
}

func TestConnectionPostgresql(t *testing.T) {
}

func TestConnectionMysql(t *testing.T) {

}
