package client

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mitchellh/mapstructure"
	"github.com/sillyhatxu/retry-utils"
	"github.com/sirupsen/logrus"
	"log"
	"sync"
	"time"
)

type SqliteClient struct {
	dataSourceName string
	db             *sql.DB
	config         *Config
	mu             sync.Mutex
}

func NewSqliteClient(dataSourceName string, opts ...Option) *SqliteClient {
	//default
	config := &Config{
		maxIdleConns:    50,
		maxOpenConns:    100,
		connMaxLifetime: 24 * time.Hour,
		attempts:        3,
		delay:           200 * time.Millisecond,
		ddlPath:         "",
		flyway:          false,
	}
	for _, opt := range opts {
		opt(config)
	}
	sc := &SqliteClient{
		dataSourceName: dataSourceName,
		config:         config,
	}
	return sc
}

func (sc *SqliteClient) Initial() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	db, err := sc.OpenDataSource()
	if err != nil {
		logrus.Errorf("sc.OpenDataSource error; MysqlClient : %#v,%v", sc, err)
		return err
	}
	sc.db = db
	err = sc.Ping()
	if err != nil {
		logrus.Errorf("ping database error; %v", err)
		return err
	}
	err = sc.initialFlayway()
	if err != nil {
		logrus.Errorf("initial flayway error; %v", err)
		return err
	}
	return nil
}

func (sc *SqliteClient) OpenDataSource() (*sql.DB, error) {
	var resultDB *sql.DB
	err := retry.Do(func() error {
		db, err := sql.Open("sqlite3", sc.dataSourceName)
		if err != nil {
			return err
		}
		resultDB = db
		return nil
	}, retry.ErrorCallback(func(n uint, err error) {
		logrus.Errorf("retry [%d] open data source error. %v", n, err)
	}))
	if err != nil {
		return nil, err
	}
	if resultDB == nil {
		return nil, fmt.Errorf("open datasource error. db is nil. %#v", sc)
	}
	return resultDB, nil
}

func (sc *SqliteClient) Ping() error {
	return sc.db.Ping()
}

func (sc *SqliteClient) GetDB() (*sql.DB, error) {
	if err := sc.Ping(); err != nil {
		logrus.Errorf("ping database error. %v", err)
		db, err := sc.OpenDataSource()
		if err != nil {
			logrus.Errorf("mc.OpenDataSource error; SqliteClient : %#v,%v", sc, err)
			return nil, err
		}
		sc.db = db
	}
	return sc.db, nil
}

func (sc *SqliteClient) ExecDDL(ddl string) error {
	db, err := sc.GetDB()
	if err != nil {
		return err
	}
	logrus.Infof("exec ddl : ")
	logrus.Infof(ddl)
	logrus.Infof("--------------------")
	_, err = db.Exec(ddl)
	return err
}

func (sc *SqliteClient) Find(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	db, err := sc.GetDB()
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		logrus.Errorf("sqlite client get transaction error. %v", err)
		return nil, err
	}
	defer tx.Commit()
	rows, err := tx.Query(sql, args...)
	if err != nil {
		logrus.Errorf("query error. %v", err)
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		logrus.Errorf("rows.Columns() error. %v", err)
		return nil, err
	}
	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}
	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for k, v := range values {
			key := columns[k]
			row[key] = string(v)
		}
		results = append(results, row)
	}
	return results, nil
}

func (sc *SqliteClient) Insert(sql string, args ...interface{}) (int64, error) {
	db, err := sc.GetDB()
	if err != nil {
		return 0, nil
	}
	stm, err := db.Prepare(sql)
	if err != nil {
		logrus.Errorf("prepare sqlite error. %v", err)
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		logrus.Errorf("insert data error. %v", err)
		return 0, err
	}
	return result.LastInsertId()
}

func (sc *SqliteClient) Update(sql string, args ...interface{}) (int64, error) {
	db, err := sc.GetDB()
	if err != nil {
		return 0, nil
	}
	stm, err := db.Prepare(sql)
	if err != nil {
		logrus.Errorf("prepare sqlite error. %v", err)
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		logrus.Errorf("update data error. %v", err)
		return 0, err
	}
	return result.RowsAffected()
}

func (sc *SqliteClient) Delete(sql string, args ...interface{}) (int64, error) {
	db, err := sc.GetDB()
	if err != nil {
		return 0, nil
	}
	stm, err := db.Prepare(sql)
	if err != nil {
		logrus.Errorf("prepare sqlite error. %v", err)
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		logrus.Errorf("delete data error. %v", err)
		return 0, err
	}
	return result.RowsAffected()
}

type TransactionCallback func(*sql.Tx) error

func (sc *SqliteClient) Transaction(callback TransactionCallback) error {
	db, err := sc.GetDB()
	if err != nil {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		logrus.Errorf("sqlite client get transaction error. %v", err)
		return err
	}
	err = callback(tx)
	if err != nil {
		logrus.Errorf("transaction data error. %v", err)
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

type FieldFunc func(rows *sql.Rows) error

func (sc *SqliteClient) Query(query string, fieldFunc FieldFunc, args ...interface{}) error {
	db, err := sc.GetDB()
	if err != nil {
		return err
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err := fieldFunc(rows)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func (sc *SqliteClient) FindMapFirst(sql string, args ...interface{}) (map[string]interface{}, error) {
	array, err := sc.FindMapArray(sql, args...)
	if err != nil {
		return nil, err
	}
	if array == nil || len(array) == 0 {
		return nil, nil
	}
	return array[0], nil
}

func (sc *SqliteClient) FindMapArray(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	db, err := sc.GetDB()
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Println("sqlite client get transaction error.", err)
		return nil, err
	}
	defer tx.Commit()
	rows, err := tx.Query(sql, args...)
	if err != nil {
		log.Println("Query error.", err)
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		log.Println("rows.Columns() error.", err)
		return nil, err
	}
	//values是每个列的值，这里获取到byte里
	values := make([][]byte, len(columns))
	//query.Scan的参数，因为每次查询出来的列是不定长的，用len(cols)定住当次查询的长度
	scans := make([]interface{}, len(columns))
	//让每一行数据都填充到[][]byte里面
	for i := range values {
		scans[i] = &values[i]
	}
	//最后得到的map
	var results []map[string]interface{}
	for rows.Next() { //循环，让游标往下推
		if err := rows.Scan(scans...); err != nil { //query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			return nil, err
		}
		row := make(map[string]interface{}) //每行数据
		for k, v := range values {          //每行数据是放在values里面，现在把它挪到row里
			key := columns[k]
			//valueType := reflect.TypeOf(v)
			//log.Info(valueType)
			row[key] = string(v)
		}
		results = append(results, row)
	}
	return results, nil
}

func (sc *SqliteClient) FindList(sql string, input interface{}, args ...interface{}) error {
	results, err := sc.FindMapArray(sql, args...)
	if err != nil {
		return err
	}
	config := &mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeHookFunc("2006-01-02 15:04:05"),
		WeaklyTypedInput: true,
		Result:           input,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	err = decoder.Decode(results)
	if err != nil {
		return err
	}
	return nil
}

func (sc *SqliteClient) FindListByConfig(sql string, config *mapstructure.DecoderConfig, args ...interface{}) error {
	results, err := sc.FindMapArray(sql, args...)
	if err != nil {
		return err
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	err = decoder.Decode(results)
	if err != nil {
		return err
	}
	return nil
}

func (sc *SqliteClient) FindFirst(sql string, input interface{}, args ...interface{}) error {
	result, err := sc.FindMapFirst(sql, args...)
	if err != nil {
		return err
	}
	config := &mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeHookFunc("2006-01-02 15:04:05"),
		WeaklyTypedInput: true,
		Result:           input,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	err = decoder.Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (sc *SqliteClient) FindFirstByConfig(sql string, config *mapstructure.DecoderConfig, args ...interface{}) error {
	result, err := sc.FindMapFirst(sql, args...)
	if err != nil {
		return err
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	err = decoder.Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (sc *SqliteClient) Count(sql string, args ...interface{}) (int64, error) {
	db, err := sc.GetDB()
	if err != nil {
		return 0, err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Println("sqlite client get connection error.", err)
		return 0, err
	}
	defer tx.Commit()
	var count int64
	countErr := tx.QueryRow(sql, args...).Scan(&count)
	if countErr != nil {
		log.Println("Query count error.", err)
		return 0, err
	}
	return count, nil
}
