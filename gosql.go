package gosql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ErrorRowNotRecognized = fmt.Errorf("row not recognized")
	ErrorTypeNotSupported = fmt.Errorf("type not supported")
	// internal
	defaultTag = "sql"
)

type Orm interface {
	RegisterTable(row interface{}, cfg *TableConfig) error
	Insert(row interface{}) (lastID int64, err error)
	Update(row interface{}) (err error)
	Select(row interface{}, query string, args ...interface{}) (rows []interface{}, err error)
	Delete(row interface{}) (err error)
}

type TableConfig struct {
	Host          string
	Username      string
	Password      string
	Port          string
	DBName        string
	TableName     string
	PrimaryKey    string
	AutoIncrement bool
	db            *sql.DB
}

type orm struct {
	tag      string
	lock     sync.RWMutex
	tableMap map[string]*TableConfig
}

func New(tag string) Orm {
	if len(tag) == 0 {
		tag = defaultTag
	}
	return &orm{
		tag:      tag,
		tableMap: make(map[string]*TableConfig),
	}
}

func (o *orm) getTableConfig(row interface{}) (*TableConfig, error) {
	o.lock.RLock()
	defer o.lock.RUnlock()
	typeStr := reflect.TypeOf(row).String()
	cfg, exists := o.tableMap[typeStr]
	if !exists {
		return nil, ErrorRowNotRecognized
	}
	return cfg, nil
}

func (o *orm) extractRow(row interface{}) (keys []string, values []interface{}, colMap map[int]bool) {
	colMap = make(map[int]bool)
	v := reflect.ValueOf(row)
	v = reflect.Indirect(v)
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		ft := t.Field(i)
		fv := v.Field(i)
		tag := ft.Tag.Get(o.tag)
		strs := strings.Split(tag, ",")
		if len(tag) > 0 && len(strs) > 0 && strs[0] != "-" {
			key := strs[0]
			keys = append(keys, key)
			values = append(values, fv.Interface())
			colMap[i] = true
		}
	}
	return keys, values, colMap
}

func (o *orm) RegisterTable(row interface{}, cfg *TableConfig) error {
	o.lock.Lock()
	defer o.lock.Unlock()
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.DBName,
	))
	if err != nil {
		return err
	}
	if err = db.Ping(); err != nil {
		return err
	}
	cfg.db = db
	typeStr := reflect.TypeOf(row).String()
	o.tableMap[typeStr] = cfg
	return nil
}

func (o *orm) Insert(row interface{}) (lastID int64, err error) {
	cfg, err := o.getTableConfig(row)
	if err != nil {
		return 0, err
	}
	keyQuery := ""
	valueQuery := ""
	selectedValeus := []interface{}{}
	keys, values, _ := o.extractRow(row)
	for i, key := range keys {
		if cfg.AutoIncrement && cfg.PrimaryKey == key {
			continue
		}
		keyQuery += fmt.Sprintf("`%s`,", key)
		valueQuery += "?,"
		selectedValeus = append(selectedValeus, values[i])
	}
	if len(keyQuery) > 0 {
		keyQuery = keyQuery[:len(keyQuery)-1]
	}
	if len(valueQuery) > 0 {
		valueQuery = valueQuery[:len(valueQuery)-1]
	}
	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES (%s)",
		cfg.TableName,
		keyQuery,
		valueQuery,
	)
	result, err := cfg.db.Exec(query, selectedValeus...)
	if err != nil {
		return 0, err
	}
	lastID, err = result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return lastID, nil
}

func (o *orm) Update(row interface{}) (err error) {
	cfg, err := o.getTableConfig(row)
	if err != nil {
		return err
	}
	keyQuery := ""
	selectedValeus := []interface{}{}
	var primaryValue interface{}
	keys, values, _ := o.extractRow(row)
	for i, key := range keys {
		if cfg.PrimaryKey == key {
			primaryValue = values[i]
		} else {
			keyQuery += fmt.Sprintf("`%s`=?,", key)
			selectedValeus = append(selectedValeus, values[i])
		}
	}
	if len(keyQuery) > 0 {
		keyQuery = keyQuery[:len(keyQuery)-1]
	}
	query := fmt.Sprintf(
		"UPDATE `%s` SET %s WHERE %s=?",
		cfg.TableName,
		keyQuery,
		cfg.PrimaryKey,
	)
	selectedValeus = append(selectedValeus, primaryValue)
	if _, err = cfg.db.Exec(query, selectedValeus...); err != nil {
		return err
	}
	return nil
}

func (o *orm) Select(row interface{}, query string, args ...interface{}) (rows []interface{}, err error) {
	cfg, err := o.getTableConfig(row)
	if err != nil {
		return nil, err
	}
	keyQuery := ""
	keys, _, colMap := o.extractRow(row)
	for _, key := range keys {
		keyQuery += fmt.Sprintf("`%s`,", key)
	}
	if len(keyQuery) > 0 {
		keyQuery = keyQuery[:len(keyQuery)-1]
	}
	if len(query) == 0 {
		query = "TRUE"
	}
	sqlQuery := fmt.Sprintf(
		"SELECT %s FROM `%s` WHERE %s",
		keyQuery,
		cfg.TableName,
		query,
	)
	sqlRows, err := cfg.db.Query(sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	v := reflect.ValueOf(row).Elem()
	for sqlRows.Next() {
		var dest []interface{}
		for i := 0; i < v.NumField(); i++ {
			if !colMap[i] {
				continue
			}
			a := reflect.New(v.Field(i).Type())
			dest = append(dest, a.Interface())
		}
		if err := sqlRows.Scan(dest...); err != nil {
			return nil, err
		}
		index := 0
		ptr := reflect.New(v.Type())
		for i := 0; i < v.NumField(); i++ {
			if !colMap[i] {
				continue
			}
			ptr.Elem().Field(i).Set(reflect.ValueOf(dest[index]).Elem())
			index++
		}
		rows = append(rows, ptr.Interface())
	}
	return rows, nil
}

func (o *orm) Delete(row interface{}) (err error) {
	cfg, err := o.getTableConfig(row)
	if err != nil {
		return err
	}
	var primaryValue interface{}
	keys, values, _ := o.extractRow(row)
	for i, key := range keys {
		if cfg.PrimaryKey == key {
			primaryValue = values[i]
			break
		}
	}
	query := fmt.Sprintf(
		"DELETE FROM `%s` WHERE `%s`=?",
		cfg.TableName,
		cfg.PrimaryKey,
	)
	if _, err = cfg.db.Exec(query, primaryValue); err != nil {
		return err
	}
	return nil
}
