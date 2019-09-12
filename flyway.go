package client

import (
	"database/sql"
	"fmt"
	"github.com/sirupsen/logrus"
	"hash/fnv"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	SchemaVersionStatusSuccess = `SUCCESS`

	SchemaVersionStatusError = `ERROR`

	SqliteMasterSQL = `
		SELECT count(1) FROM sqlite_master WHERE type='table' AND name = ?
	`

	InsertSchemaVersionSQL = `
		INSERT INTO schema_version (script, checksum, execution_time, status) values (?, ?, ?, ?)
	`
	DDLSchemaVersion = `
		CREATE TABLE IF NOT EXISTS schema_version
		(
		  id             INTEGER PRIMARY KEY AUTOINCREMENT,
		  script         TEXT    NOT NULL,
		  checksum       TEXT    NOT NULL,
		  execution_time TEXT    NOT NULL,
		  status         TEXT    NOT NULL,
		  created_time   datetime default current_timestamp
		);
	`
)

type SchemaVersion struct {
	Id            int64
	Script        string
	Checksum      string
	ExecutionTime string
	Status        string
	CreatedTime   time.Time
}

func (sc *SqliteClient) findByScript(script string, svArray []SchemaVersion) (bool, *SchemaVersion) {
	for _, sv := range svArray {
		if sv.Script == script {
			return true, &sv
		}
	}
	return false, nil
}

func (sc *SqliteClient) hasError(svArray []SchemaVersion) error {
	for _, sv := range svArray {
		if sv.Status == SchemaVersionStatusError {
			return fmt.Errorf("schema version has abnormal state. You need to prioritize exceptional states. %#v", sv)
		}
	}
	return nil
}

func hash64(s string) (uint64, error) {
	h := fnv.New64()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}

func shortDur(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}

func (sc *SqliteClient) initialFlayway() (err error) {
	if !sc.config.flyway {
		return nil
	}
	err = sc.initialSchemaVersion()
	if err != nil {
		return err
	}
	err = sc.executeFlayway()
	if err != nil {
		return err
	}
	return nil
}

func (sc *SqliteClient) executeFlayway() error {
	files, err := ioutil.ReadDir(sc.config.ddlPath)
	if err != nil {
		return nil
	}
	svArray, err := sc.SchemaVersionArray()
	if err != nil {
		return err
	}
	err = sc.hasError(svArray)
	if err != nil {
		return err
	}
	for _, f := range files {
		err := sc.readFile(f, svArray)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *SqliteClient) readFile(fileInfo os.FileInfo, svArray []SchemaVersion) error {
	b, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", sc.config.ddlPath, fileInfo.Name()))
	if err != nil {
		return err
	}
	checksum, err := hash64(string(b))
	if err != nil {
		return err
	}
	exist, sv := sc.findByScript(fileInfo.Name(), svArray)
	if exist {
		if sv.Checksum != strconv.FormatUint(checksum, 10) {
			return fmt.Errorf("sql file has been changed. %#v", sv)
		}
		return nil
	}
	execTime := time.Now()
	schemaVersion := SchemaVersion{
		Script:   fileInfo.Name(),
		Checksum: strconv.FormatUint(checksum, 10),
		Status:   SchemaVersionStatusError,
	}
	err = sc.ExecDDL(string(b))
	if err == nil {
		schemaVersion.Status = SchemaVersionStatusSuccess
	}
	elapsed := time.Since(execTime)
	schemaVersion.ExecutionTime = shortDur(elapsed)
	sc.insertSchemaVersion(schemaVersion)
	if err != nil {
		return err
	}
	return nil
}

func (sc *SqliteClient) insertSchemaVersion(schemaVersion SchemaVersion) {
	_, err := sc.Insert(InsertSchemaVersionSQL, schemaVersion.Script, schemaVersion.Checksum, schemaVersion.ExecutionTime, schemaVersion.Status)
	if err != nil {
		logrus.Errorf("insert schema version error. %v", err)
	}
}

func (sc *SqliteClient) initialSchemaVersion() error {
	exist, err := sc.HasTable("schema_version")
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	return sc.ExecDDL(DDLSchemaVersion)
}

func (sc *SqliteClient) HasTable(table string) (bool, error) {
	var count int
	err := sc.Query(SqliteMasterSQL, func(rows *sql.Rows) error {
		return rows.Scan(&count)
	}, table)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (sc *SqliteClient) SchemaVersionArray() ([]SchemaVersion, error) {
	var svArray []SchemaVersion
	err := sc.Query(`select * from schema_version`, func(rows *sql.Rows) error {
		var sv SchemaVersion
		err := rows.Scan(&sv.Id, &sv.Script, &sv.Checksum, &sv.ExecutionTime, &sv.Status, &sv.CreatedTime)
		svArray = append(svArray, sv)
		return err
	})
	if err != nil {
		return nil, err
	}
	if svArray == nil {
		svArray = make([]SchemaVersion, 0)
	}
	return svArray, nil
}
