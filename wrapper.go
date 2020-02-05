package wdb

import (
	"database/sql"
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	"os"
)

type wrapperDB struct {
	db     *sqlx.DB
	dbaddr string
	dbuser string
	dbpass string
	dbport string
	dbname string
	log    Logger
}

type Logger interface {
	Info(args ...interface{})
	Error(args ...interface{})
}

var (
	running bool
	wdb     = wrapperDB{}
)

func Select(dest interface{}, query string, args ...interface{}) error {
	return wdb.Select(dest, query, args...)
}

func Get(dest interface{}, query string, args ...interface{}) error {
	return wdb.Get(dest, query, args...)
}

func Exec(query string, args ...interface{}) (sql.Result, error) {
	return wdb.Exec(query, args...)
}

func Rebind(query string) string {
	return wdb.Rebind(query)
}

func MustExec(query string, args ...interface{}) sql.Result {
	return wdb.MustExec(query, args...)
}

func (w *wrapperDB) Select(dest interface{}, query string, args ...interface{}) error {
	if !PingDB() {
		return errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Select(dest, query, args...)
}

func (w *wrapperDB) Get(dest interface{}, query string, args ...interface{}) error {
	if !PingDB() {
		return errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Get(dest, query, args...)
}

func (w *wrapperDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	if !PingDB() {
		return nil, errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Exec(query, args...)
}

func (w *wrapperDB) MustExec(query string, args ...interface{}) sql.Result {
	if !PingDB() {
		return nil
	}
	return w.db.MustExec(query, args...)
}

func (w *wrapperDB) Rebind(query string) string {
	PingDB()
	return w.db.Rebind(query)
}

func New(dbaddr string, dbuser string, dbpass string, dbport string, dbname string, log Logger) {
	wdb.dbaddr = dbaddr
	wdb.dbuser = dbuser
	wdb.dbpass = dbpass
	wdb.dbport = dbport
	wdb.dbname = dbname
	wdb.log = log
	Connect()
}

func Connect() bool {
	if running {
		return false
	}
	running = true
	var err error
	if os.Getenv("DOCKER") == "1" {
		wdb.dbaddr = "db"
	} else {
		wdb.dbaddr = viper.GetString("app.db.addr")
	}
	for {
		wdb.log.Info("Попытка подключения к базе данных")
		wdb.db, err = Dial()
		if err != nil {
			continue
		} else {
			wdb.log.Info("К БД подключилсь")
			break
		}
	}
	running = false
	return true
}

func Dial() (dbx *sqlx.DB, err error) {
	dbx, err = sqlx.Connect(
		"mysql",
		wdb.dbuser+":"+wdb.dbpass+"@tcp("+wdb.dbaddr+":"+wdb.dbport+")/"+wdb.dbname+"?charset=utf8mb4,utf8")
	if err != nil {
		wdb.log.Error("error connection db ", err)
		return
	}
	dbx.SetMaxOpenConns(10)
	wdb.log.Info("Posts DB started")
	return dbx, nil
}

func PingDB() bool {
	err := wdb.db.Ping()
	if err != nil {
		wdb.log.Error("error 1 PingDB ", err)
		if err := wdb.db.Close(); err != nil {
			wdb.log.Error("error 2 PingDB ", err)
		}
		return Connect()
	}
	return true
}

func GetPtr() *wrapperDB {
	return &wdb
}
