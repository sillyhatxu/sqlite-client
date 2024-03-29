package client

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

//const path = "/Users/shikuanxu/go/src/github.com/sillyhatxu/mini-mq"

const path = "/Users/cookie/go/gopath/src/github.com/sillyhatxu/mini-mq"

var Client *SqliteClient

func init() {
	Client = NewSqliteClient("./test.db", DDLPath(fmt.Sprintf("%s/db/migration", path)))
	err := Client.Initial()
	if err != nil {
		logrus.Error(err)
		return
	}
}

func TestSqliteClient_Initial(t *testing.T) {
	db, err := Client.GetDB()
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestSqliteClient_SchemaVersion(t *testing.T) {
	svArray, err := Client.SchemaVersionArray()
	assert.Nil(t, err)
	for _, sv := range svArray {
		logrus.Infof("%#v", sv)
	}
}

func TestSqliteClient_Find(t *testing.T) {
	array, err := Client.Find(`select * from user_info`)
	assert.Nil(t, err)
	for _, a := range array {
		logrus.Infof("%#v", a)
	}
}

func Test(t *testing.T) {
	t1 := time.Now()
	for i := 0; i < 1000; i++ {
		time.Sleep(500)
	}
	elapsed := time.Since(t1)
	fmt.Println("App elapsed: ", elapsed)
	fmt.Println(string(elapsed))
}
