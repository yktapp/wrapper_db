package wdb

import (
	"database/sql"
	"errors"
	"github.com/jmoiron/sqlx"
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
}

type Logger interface {
	Info(args ...interface{})
	Error(args ...interface{})
}

var (
	running bool
	_wdb     = WrapperDB{}
	_wdbm    = []WrapperDB{}
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

func QueryRow(query string, args ...interface{}) *sql.Row {
	return _wdb.QueryRow(query, args...)
}

func (w *WrapperDB) Select(dest interface{}, query string, args ...interface{}) error {
	if !pingDB(w) {
		return errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Select(dest, query, args...)
}

func (w *WrapperDB) Get(dest interface{}, query string, args ...interface{}) error {
	if !pingDB(w) {
		return errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Get(dest, query, args...)
}

func (w *WrapperDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	if !pingDB(w) {
		return nil, errors.New("Ошибка подключения к базе данных")
	}
	return w.db.Exec(query, args...)
}

func (w *WrapperDB) MustExec(query string, args ...interface{}) sql.Result {
	if !pingDB(w) {
		return nil
	}
	return w.db.MustExec(query, args...)
}

func (w *WrapperDB) Rebind(query string) string {
	pingDB(w)
	return w.db.Rebind(query)
}

func (w *WrapperDB) QueryRow(query string, args ...interface{}) *sql.Row {
	pingDB(w)
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

func connect(wdb *WrapperDB) (conn *sqlx.DB, err error) {
	if running {
		return conn, err
	}
	running = true
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
	running = false
	return conn, err
}

func dial(wdb *WrapperDB) (conn *sqlx.DB, err error) {
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
		conn, err = sqlx.Connect("postgres", "host=" + wdb.dbaddr + " port=" + wdb.dbport + " password=" + wdb.dbpass + " user=" + wdb.dbuser + " dbname=" + wdb.dbname + " sslmode=disable")
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