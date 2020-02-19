package wdb

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"sync"
)

type WrapperDB struct {
	db     *sqlx.DB
	dbaddr string
	dbuser string
	dbpass string
	dbport string
	dbname string
	drname string
	log    Logger
	mux    sync.Mutex
}

type Logger interface {
	Info(args ...interface{})
	Error(args ...interface{})
}

var (
	running bool
	_wdb    = WrapperDB{}
	_wdbm   = []WrapperDB{}
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

func QueryRow(query string, args ...interface{}) *sql.Row {
	return _wdb.QueryRow(query, args...)
}

func Query(query string, args ...interface{}) (*sql.Rows, error) {
	return _wdb.Query(query, args...)
}

func (w *WrapperDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	r, err := w.db.Query(query, args...)
	if err == driver.ErrBadConn || err == mysql.ErrInvalidConn {
		err = w.db.Close()
		if err != nil {
			w.log.Error("error on close connection - ", err)
			return r, err
		}
		w.db, err = connect(w)
		if err != nil {
			w.log.Error("error on init connection - ", err)
			return r, err
		}
	}
	return r, err
}

func (w *WrapperDB) Select(dest interface{}, query string, args ...interface{}) error {
	err := w.db.Select(dest, query, args...)
	if err == driver.ErrBadConn || err == mysql.ErrInvalidConn {
		err = w.db.Close()
		if err != nil {
			w.log.Error("error on close connection - ", err)
			return err
		}
		w.db, err = connect(w)
		if err != nil {
			w.log.Error("error on init connection - ", err)
			return err
		}
	}
	return err
}

func (w *WrapperDB) Get(dest interface{}, query string, args ...interface{}) error {
	err := w.db.Get(dest, query, args...)
	if err == driver.ErrBadConn || err == mysql.ErrInvalidConn {
		err = w.db.Close()
		if err != nil {
			w.log.Error("error on close connection - ", err)
			return err
		}
		w.db, err = connect(w)
		if err != nil {
			w.log.Error("error on init connection - ", err)
			return err
		}
	}
	return err
}

func (w *WrapperDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	r, err := w.db.Exec(query, args...)
	if err == driver.ErrBadConn || err == mysql.ErrInvalidConn {
		err = w.db.Close()
		if err != nil {
			w.log.Error("error on close connection - ", err)
			return r, err
		}
		w.db, err = connect(w)
		if err != nil {
			w.log.Error("error on init connection - ", err)
			return r, err
		}
	}
	return r, err
}

func (w *WrapperDB) Rebind(query string) string {
	return w.db.Rebind(query)
}

func (w *WrapperDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return w.db.QueryRow(query, args...)
}

func New(dbaddr string, dbuser string, dbpass string, dbport string, dbname string, log Logger) {
	_wdb.dbaddr = dbaddr
	_wdb.dbuser = dbuser
	_wdb.dbpass = dbpass
	_wdb.dbport = dbport
	_wdb.dbname = dbname
	_wdb.drname = "mysql"
	_wdb.log = log
	_wdb.db, _ = connect(&_wdb)
}

func NewWithClient(dbaddr string, dbuser string, dbpass string, dbport string, dbname string, log Logger, c *sqlx.DB) {
	_wdb.dbaddr = dbaddr
	_wdb.dbuser = dbuser
	_wdb.dbpass = dbpass
	_wdb.dbport = dbport
	_wdb.dbname = dbname
	_wdb.drname = "mysql"
	_wdb.log = log
	_wdb.db = c
}

func SetDriver(wdb *WrapperDB, drname string) {
	wdb.drname = drname
}

func NewMulti(dbaddr string, dbuser string, dbpass string, dbport string, dbname string, drname string, log Logger) (*WrapperDB, error) {
	wdb := &WrapperDB{
		db:     nil,
		dbaddr: dbaddr,
		dbuser: dbuser,
		dbpass: dbpass,
		dbport: dbport,
		dbname: dbname,
		drname: drname,
		log:    log,
	}
	var err error
	wdb.db, err = connect(wdb)
	if err != nil {
		return wdb, err
	}
	wdb.log = log
	_wdbm = append(_wdbm, *wdb)
	return wdb, nil
}

func NewMultiWithClient(dbaddr string, dbuser string, dbpass string, dbport string, dbname string, drname string, log Logger, c *sqlx.DB) (*WrapperDB, error) {
	wdb := &WrapperDB{
		db:     c,
		dbaddr: dbaddr,
		dbuser: dbuser,
		dbpass: dbpass,
		dbport: dbport,
		dbname: dbname,
		drname: drname,
		log:    log,
	}
	return wdb, nil
}

func connect(wdb *WrapperDB) (conn *sqlx.DB, err error) {
	for {
		wdb.log.Info("Попытка подключения к базе данных")
		conn, err = dial(wdb)
		if err != nil {
			continue
		} else {
			wdb.log.Info("К БД подключилсь")
			break
		}
	}
	return conn, err
}

func dial(wdb *WrapperDB) (conn *sqlx.DB, err error) {
	switch wdb.drname {
	case "mysql":
		conn, err = sqlx.Connect(
			"mysql",
			wdb.dbuser+":"+wdb.dbpass+"@tcp("+wdb.dbaddr+":"+wdb.dbport+")/"+wdb.dbname+"?charset=utf8mb4,utf8&parseTime=true")
		if err != nil {
			wdb.log.Error("error connection db ", err)
			return conn, err
		}
		conn.SetMaxOpenConns(10)
		wdb.log.Info("Posts DB started")
		return conn, nil
	case "postgresql":
		conn, err = sqlx.Connect("postgres", "host="+wdb.dbaddr+" port="+wdb.dbport+" password="+wdb.dbpass+" user="+wdb.dbuser+" dbname="+wdb.dbname+" sslmode=disable")
		if err != nil {
			wdb.log.Error("error connection db pg", err)
			return conn, err
		}
		return conn, nil
	default:
		return conn, errors.New("set db driver first")
	}
}

func pingDB(wdb *WrapperDB) bool {
	wdb.mux.Lock()
	defer wdb.mux.Unlock()
	err := wdb.db.Ping()
	if err != nil {
		wdb.log.Error("error 1 PingDB ", err)
		if err := wdb.db.Close(); err != nil {
			wdb.log.Error("error 2 PingDB ", err)
		}
		wdb.db, err = connect(wdb)
	}
	return true
}

func GetPtr() *WrapperDB {
	return &_wdb
}

func GetDb(i int) *WrapperDB {
	return &_wdbm[i]
}
