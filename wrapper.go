package wdb

import (
	"database/sql"
	"errors"
	"os"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

type wrapperDB struct {
	db     *sqlx.DB
	dbaddr string
	dbuser string
	dbpass string
	dbport string
	dbname string
	drname string
	log    Logger
}

type Logger interface {
	Info(args ...interface{})
	Error(args ...interface{})
}

var (
	running bool
	_wdb     = wrapperDB{}
	_wdbm    = []wrapperDB{}
)

func Select(dest interface{}, query string, args ...interface{}) error {
	return _wdb.Select(dest, query, args...)
}

func Get(dest interface{}, query string, args ...interface{}) error {
	return _wdb.Get(dest, query, args...)
}

func Exec(query string, args ...interface{}) (sql.Result, error) {
	return _wdb.Exec(query, args...)
}

func Rebind(query string) string {
	return _wdb.Rebind(query)
}

func MustExec(query string, args ...interface{}) sql.Result {
	return _wdb.MustExec(query, args...)
}

func (w *wrapperDB) Select(dest interface{}, query string, args ...interface{}) error {
	if !PingDB(w) {
		return errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Select(dest, query, args...)
}

func (w *wrapperDB) Get(dest interface{}, query string, args ...interface{}) error {
	if !PingDB(w) {
		return errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Get(dest, query, args...)
}

func (w *wrapperDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	if !PingDB(w) {
		return nil, errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Exec(query, args...)
}

func (w *wrapperDB) MustExec(query string, args ...interface{}) sql.Result {
	if !PingDB(w) {
		return nil
	}
	return w.db.MustExec(query, args...)
}

func (w *wrapperDB) Rebind(query string) string {
	PingDB(w)
	return w.db.Rebind(query)
}

func New(dbaddr string, dbuser string, dbpass string, dbport string, dbname string, log Logger) {
	_wdb.db, _ = Connect(&wrapperDB{
		db:     nil,
		dbaddr: dbaddr,
		dbuser: dbuser,
		dbpass: dbpass,
		dbport: dbport,
		dbname: dbname,
		drname: "mysql",
		log:    log,
	})
	_wdb.log = log
}

func SetDriver(wdb *wrapperDB, drname string) {
	wdb.drname = drname
}

func NewMulti(dbaddr string, dbuser string, dbpass string, dbport string, dbname string, log Logger) {
	wdb := &wrapperDB{
		db:     nil,
		dbaddr: dbaddr,
		dbuser: dbuser,
		dbpass: dbpass,
		dbport: dbport,
		dbname: dbname,
		drname: "mysql",
		log:    nil,
	}
	wdb.db, _ = Connect(wdb)
	wdb.log = log
	_wdbm = append(_wdbm, *wdb)
}

func Connect(wdb *wrapperDB) (conn *sqlx.DB, err error) {
	if running {
		return conn, err
	}
	running = true
	if os.Getenv("DOCKER") == "1" {
		wdb.dbaddr = "db"
	} else {
		wdb.dbaddr = viper.GetString("app.db.addr")
	}
	for {
		wdb.log.Info("Попытка подключения к базе данных")
		wdb.db, err = Dial(wdb)
		if err != nil {
			continue
		} else {
			wdb.log.Info("К БД подключилсь")
			break
		}
	}
	running = false
	return conn, err
}

func Dial(wdb *wrapperDB) (conn *sqlx.DB, err error) {
	switch wdb.drname {
	case "mysql":
		conn, err = sqlx.Connect(
			"mysql",
			wdb.dbuser+":"+wdb.dbpass+"@tcp("+wdb.dbaddr+":"+wdb.dbport+")/"+wdb.dbname+"?charset=utf8mb4,utf8")
		if err != nil {
			wdb.log.Error("error connection db ", err)
			return conn, err
		}
		conn.SetMaxOpenConns(10)
		wdb.log.Info("Posts DB started")
		return conn, nil
	case "postgresql":
		conn, err = sqlx.Connect("postgres", "host=" + wdb.dbaddr + " port=" + wdb.dbport + " user=" + wdb.dbuser + " dbname=" + wdb.dbname + " sslmode=disable")
		if err != nil {
			wdb.log.Error("error connection db pg", err)
			return conn, err
		}
		return conn, nil
	default:
		return conn, errors.New("set db driver first")
	}
}

func PingDB(wdb *wrapperDB) bool {
	err := wdb.db.Ping()
	if err != nil {
		wdb.log.Error("error 1 PingDB ", err)
		if err := wdb.db.Close(); err != nil {
			wdb.log.Error("error 2 PingDB ", err)
		}
		_, err = Connect(wdb)
	}
	return true
}

func GetPtr() *wrapperDB {
	return &_wdb
}

func GetDb(i int) *wrapperDB {
	return &_wdbm[i]
}