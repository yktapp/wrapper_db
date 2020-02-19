package wdb

import (
	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)


// Without multi

func TestExec(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()

	// Mock
	mock.ExpectExec("INSERT INTO sometable").WillReturnResult(sqlmock.NewResult(1, 1))
	// Connect
	NewWithClient("adfsjlk", "askdf", "pass", "123", "asdf", logrus.New(), sqlxDB)
	// Test
	res, err := Exec("INSERT INTO sometable")
	if err != nil {
		t.Error("test fail")
	}
	// Result
	id, _ := res.LastInsertId()
	assert.Equal(t, int64(1), id)
}

func TestSelect(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()

	// Mock
	rows := mock.NewRows([]string{"id", "title"}).AddRow(1, "one").AddRow(2, "two")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	// Connect
	NewWithClient("adfsjlk", "askdf", "pass", "123", "asdf", logrus.New(), sqlxDB)
	// Test
	type scanned struct {
		Id int `sql:"id"`
		Title string `sql:"title"`
	}
	s := []scanned{}
	Select(&s, "SELECT")
	// Result
	var expect []scanned
	expect = append(expect, scanned{1, "one"})
	expect = append(expect, scanned{2, "two"})
	assert.Equal(t, expect, s)
}

func TestQueryRow(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()
	// Mock
	rows := mock.NewRows([]string{"id"}).AddRow(1)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	// Connect
	NewWithClient("adfsjlk", "askdf", "pass", "123", "asdf", logrus.New(), sqlxDB)
	// Test
	var id int
	err := QueryRow("SELECT").Scan(&id)
	if err != nil {
		t.Error("query fail")
	}
	// Result
	assert.Equal(t, 1, id)
}

func TestQuery(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()
	// Mock
	rows := mock.NewRows([]string{"id", "title"}).AddRow(1, "one").AddRow(2, "two")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	// Connect
	NewWithClient("adfsjlk", "askdf", "pass", "123", "asdf", logrus.New(), sqlxDB)
	// Test
	rs, _ := Query("SELECT")
	// Result
	rs.Next()
	var id int
	var title string
	rs.Scan(&id, &title)
	assert.Equal(t, 1, id)
	assert.Equal(t, "one", title)
	rs.Next()
	rs.Scan(&id, &title)
	assert.Equal(t, 2, id)
	assert.Equal(t, "two", title)
}

func TestGet(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()
	// Mock
	rows := mock.NewRows([]string{"id", "title"}).AddRow(1, "one")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	// Connect
	NewWithClient("adfsjlk", "askdf", "pass", "123", "asdf", logrus.New(), sqlxDB)
	// Test
	type scanned struct {
		Id    int    `sql:"id"`
		Title string `sql:"title"`
	}
	s := scanned{}
	Get(&s, "SELECT")
	// Result
	assert.Equal(t, 1, s.Id)
	assert.Equal(t, "one", s.Title)
}


// Multi connection

func TestSelectMulti(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()

	// Mock
	rows := mock.NewRows([]string{"id", "title"}).AddRow(1, "one").AddRow(2, "two")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	// Connect
	db, _ := NewMultiWithClient("adfsjlk", "askdf", "pass", "123", "asdf", "mysql", logrus.New(), sqlxDB)
	// Test
	type scanned struct {
		Id int `sql:"id"`
		Title string `sql:"title"`
	}
	s := []scanned{}
	db.Select(&s, "SELECT")
	// Result
	var expect []scanned
	expect = append(expect, scanned{1, "one"})
	expect = append(expect, scanned{2, "two"})
	assert.Equal(t, expect, s)
}

func TestQueryMulti(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()
	// Mock
	rows := mock.NewRows([]string{"id", "title"}).AddRow(1, "one").AddRow(2, "two")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	// Connect
	db, _ := NewMultiWithClient("adfsjlk", "askdf", "pass", "123", "asdf", "mysql", logrus.New(), sqlxDB)
	// Test
	rs, _ := db.Query("SELECT")
	// Result
	rs.Next()
	var id int
	var title string
	rs.Scan(&id, &title)
	assert.Equal(t, 1, id)
	assert.Equal(t, "one", title)
	rs.Next()
	rs.Scan(&id, &title)
	assert.Equal(t, 2, id)
	assert.Equal(t, "two", title)
}

func TestGetMulti(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()
	// Mock
	rows := mock.NewRows([]string{"id", "title"}).AddRow(1, "one")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	// Connect
	db, _ := NewMultiWithClient("adfsjlk", "askdf", "pass", "123", "asdf", "mysql", logrus.New(), sqlxDB)
	// Test
	type scanned struct {
		Id    int    `sql:"id"`
		Title string `sql:"title"`
	}
	s := scanned{}
	db.Get(&s, "SELECT")
	// Result
	assert.Equal(t, 1, s.Id)
	assert.Equal(t, "one", s.Title)
}

func TestQueryRowMulti(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()
	// Mock
	rows := mock.NewRows([]string{"id"}).AddRow(1)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	// Connect
	db, _ := NewMultiWithClient("adfsjlk", "askdf", "pass", "123", "asdf", "mysql", logrus.New(), sqlxDB)
	// Test
	var id int
	err := db.QueryRow("SELECT").Scan(&id)
	if err != nil {
		t.Error("query fail")
	}
	// Result
	assert.Equal(t, 1, id)
}

func TestExecMulti(t *testing.T) {
	// Connect mock
	mockDB, mock, _ := sqlmock.New()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	defer mockDB.Close()

	// Mock
	mock.ExpectExec("INSERT INTO sometable").WillReturnResult(sqlmock.NewResult(1, 1))
	// Connect
	db, _ := NewMultiWithClient("adfsjlk", "askdf", "pass", "123", "asdf", "mysql", logrus.New(), sqlxDB)
	// Test
	res, _ := db.Exec("INSERT INTO sometable")
	// Result
	id, _ := res.LastInsertId()
	assert.Equal(t, int64(1), id)
}

