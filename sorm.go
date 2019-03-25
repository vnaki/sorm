package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kataras/iris/core/errors"
	"strconv"
	"strings"
)

/**
 * 记录SQL
 */
var UsedSql []string

/**
 * 原生值结构
 * 参数 v 原生值表达式,如 `age`+1
 */
type Value struct {
	sql string
}

/**
 * 生成SQL原生值
 * 参数 v 原生值表达式,如 `age`+1
 */
func RawValue(v string) Value {
	return Value{sql: v}
}

/*资源结构*/
type Source struct {
	dsn    string
	db     *sql.DB
	prefix string
	table  string
	alias  string
	fields []string
	wheres []string
	joins  []string
	args   []interface{}
	orders []string
	limit  int64
	offset int64
	group  string
	having string
	record bool /*是否记录SQL语句*/
}

/*实例*/
var source *Source

/**
 * 数据库资源
 */
func Intance(dsn string) *Source {
	if source == nil {
		source = &Source{dsn: dsn}
		source.connect()
	}

	return source
}

/**
 * 判断是否已实例化
 */
func IsInstantiated() bool {
	return source != nil
}

/**
 * 关闭数据库连接
 */
func Close() {
	if source != nil {
		source.Close()
	}
}

/*连接数据库*/
func (source *Source) connect() {
	db, err := sql.Open("mysql", source.dsn)

	if err != nil {
		panic(err)
	}

	/*最大空闲连接数*/
	db.SetMaxIdleConns(5)
	/*保存句柄*/
	source.db = db
}

/*关闭数据库连接*/
func (source *Source) Close() {
	if err := source.db.Close(); err != nil {
		panic(err)
	}
}

/*
 * 表前缀
 */
func (source *Source) Prefix(prefix string) *Source {
	source.prefix = prefix
	return source
}

/*
 * 指定表名称
 * 参数 table 表的名称
 * 参数 alias 表的别名
 */
func (source *Source) Table(table string) *Source {
	source.table = table
	return source
}

/*
 * 记录SQL语句
 */
func (source *Source) Record() *Source {
	source.record = true
	return source
}

/**
 * 完整表名
 */
func (source *Source) fullTable() string {
	return source.prefix + source.table
}

/*
 * 表的别名
 */
func (source *Source) Alias(alias string) *Source {
	source.alias = alias
	return source
}

/**
 * 要查询的字段(字段已处理)
 */
func (source *Source) Fields(fields string) *Source {
	if fields != "" {
		if "*" != fields {
			for _, field := range strings.Split(fields, ",") {
				source.fields = append(source.fields, fmt.Sprintf("`%s`", strings.Join(strings.Split(field, "."), "`.`")))
			}
		} else {
			source.fields = []string{"*"}
		}
	}

	return source
}

/**
 * 要查询的字段(字段未处理)
 */
func (source *Source) RawFields(fields string) *Source {
	if fields != "" {
		source.fields = strings.Split(fields, ",")
	}

	return source
}

/**
 * 查询一条数据
 */
func (source *Source) One() (map[string]string, error) {

	/*只取一条数据*/
	if source.limit == 0 {
		source.limit = 1
	}

	data, err := source.All()

	result := map[string]string{}

	if err == nil && len(data) > 0 {
		result = data[0]
	}

	return result, err
}

/**
 * 查询全部数据
 * 参数 sqlstr 要执行的SQL语句
 * 参数 args 查询变量
 */
func (source *Source) All() ([]map[string]string, error) {

	defer source.reset()

	sqlstr, err := source.compileSelectSql()

	/*错误*/
	if err != nil {
		return []map[string]string{}, err
	}

	/*查询预处理*/
	stmt, err := source.db.Prepare(sqlstr)

	/*错误*/
	if err != nil {
		return []map[string]string{}, err
	}

	rows, err := stmt.Query(source.args...)

	/*关闭Stmt*/
	defer func() {
		if err := stmt.Close(); err != nil {
			panic(err)
		}
	}()

	/*错误*/
	if err != nil {
		return []map[string]string{}, err
	}

	/*字段数组*/
	cols, err := rows.Columns()

	/*关闭Rows*/
	defer func() {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}()

	/*错误*/
	if err != nil {
		return []map[string]string{}, err
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))

	for i := range values {
		scans[i] = &values[i]
	}

	var result []map[string]string

	for rows.Next() {

		if err := rows.Scan(scans...); err != nil {
			/*错误出现跳过*/
			continue
		}

		row := make(map[string]string)

		for key, value := range values {
			row[cols[key]] = string(value)
		}

		result = append(result, row)
	}

	/*遍历过程出现的错误*/
	if err := rows.Err(); err != nil {
		return []map[string]string{}, err
	}

	return result, nil
}

/**
 * 统计数量
 */
func (source *Source) Count() (int64, error) {
	var field string = "COUNT(*)"

	result, err := source.RawFields(field).One()

	if err != nil {
		return 0, err
	}

	/*类型转换*/
	count, err := strconv.ParseInt(result[field], 10, 64)

	if err != nil {
		return 0, err
	}

	return count, nil
}

/**
 * 求和统计
 * ```
 * Sum("field,...")
 * ```
 */
func (source *Source) Sum(fields string) (map[string]int64, error) {
	return source.aggregate(fields, "SUM")
}

/**
 * 求最大值
 * ```
 * Max("field,...")
 * ```
 */
func (source *Source) Max(fields string) (map[string]int64, error) {
	return source.aggregate(fields, "MAX")
}

/**
 * 求最小值
 * ```
 * Min("field,...")
 * ```
 */
func (source *Source) Min(fields string) (map[string]int64, error) {
	return source.aggregate(fields, "MIN")
}

/**
 * 求平均值
 * ```
 * Avg("field,...")
 * ```
 */
func (source *Source) Avg(fields string) (map[string]int64, error) {
	return source.aggregate(fields, "AVG")
}

/**
 * 聚合查询
 * 参数 fields 聚合字段
 * 参数 mode   函数模式 SUM/MAX/MIN/AVG
 */
func (source *Source) aggregate(fields string, mode string) (map[string]int64, error) {
	/*切割字段*/
	splitFields := strings.Split(fields, ",")
	copyFields := []string{}

	for index, field := range splitFields {
		copyFields = append(copyFields, field)
		splitFields[index] = fmt.Sprintf("%s(`%s`)", mode, field)
	}

	sum, err := source.RawFields(strings.Join(splitFields, ",")).One()

	/*结果储存*/
	var result = map[string]int64{}

	/*错误处理*/
	if err != nil {
		return result, err
	}

	/*格式处理*/
	for index, key := range splitFields {
		if sum[key] == "" {
			result[copyFields[index]] = 0
		} else {
			value, err := strconv.ParseInt(sum[key], 10, 64)

			/*类型转换错误*/
			if err != nil {
				return result, err
			}

			result[copyFields[index]] = value
		}
	}

	return result, nil
}

/**
 * 插入数据
 * 参数 data 要插入的数据
 */
func (source *Source) Insert(data map[string]interface{}) (int64, error) {
	/*释放参数*/
	defer source.reset()

	/*SQL编译*/
	sqlstr, err := source.compileInsertSql(data)

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := source.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(source.args...)

	/*关闭Stmt*/
	defer func() {
		if err := stmt.Close(); err != nil {
			panic(err)
		}
	}()

	/*错误*/
	if err != nil {
		return 0, err
	}

	/*插入ID*/
	insertId, err := result.LastInsertId()

	/*错误*/
	if err != nil {
		return 0, err
	}

	return insertId, nil
}

/**
 * 更新数据
 * 参数 data 要更新的数据
 */
func (source *Source) Update(data map[string]interface{}) (int64, error) {
	/*释放参数*/
	defer source.reset()

	/*SQL编译*/
	sqlstr, err := source.compileUpdateSql(data)

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := source.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(source.args...)

	/*关闭Stmt*/
	defer func() {
		if err := stmt.Close(); err != nil {
			panic(err)
		}
	}()

	/*错误*/
	if err != nil {
		return 0, err
	}

	/*更新影响行数*/
	rowsAffected, err := result.RowsAffected()

	/*错误*/
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

/**
 * 更新制定字段
 */
func (source *Source) UpdateFiled(name string, value interface{}) (int64, error) {
	return source.Update(map[string]interface{}{name: value})
}

/**
 * 值加法更新 (可以同时更新多个字段值)
 */
func (source *Source) Increase(data map[string]uint64) (int64, error) {
	values := map[string]interface{}{}

	for key, value := range data {
		values[key] = RawValue(fmt.Sprintf("`%s`+%d", key, value))
	}

	return source.Update(values)
}

/**
 * 值减法更新 (可以同时更新多个字段值)
 */
func (source *Source) Decrease(data map[string]uint64) (int64, error) {
	values := map[string]interface{}{}

	for key, value := range data {
		values[key] = RawValue(fmt.Sprintf("`%s`-%d", key, value))
	}

	return source.Update(values)
}

/*删除数据*/
func (source *Source) Delete() (int64, error) {
	/*释放参数*/
	defer source.reset()

	/*SQL编译*/
	sqlstr, err := source.compileDeleteSql()

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := source.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(source.args...)

	/*关闭Stmt*/
	defer func() {
		if err := stmt.Close(); err != nil {
			panic(err)
		}
	}()

	/*错误*/
	if err != nil {
		return 0, err
	}

	/*删除影响行数*/
	rowsAffected, err := result.RowsAffected()

	/*错误*/
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

/**
 * INNER JOIN语句
 */
func (source *Source) Inner(table string, on string) *Source {
	source.Join(table, on, "INNER")
	return source
}

/*LEFT JOIN语句*/
func (source *Source) Left(table string, on string) *Source {
	source.Join(table, on, "LEFT")
	return source
}

/*RIGHT JOIN语句*/
func (source *Source) Right(table string, on string) *Source {
	source.Join(table, on, "RIGHT")
	return source
}

/*JOIN语句*/
func (source *Source) Join(table string, on string, mode string) {
	source.joins = append(source.joins, fmt.Sprintf("%s JOIN %s ON %s", mode, table, on))
}

/*数组查询条件*/
func (source *Source) Where(where map[string]interface{}) *Source {
	for field, unknown := range where {
		switch value := unknown.(type) {
		case string:
			source.Eq(field, value)
		case int:
			source.Eq(field, value)
		case int8:
			source.Eq(field, value)
		case int16:
			source.Eq(field, value)
		case int32:
			source.Eq(field, value)
		case int64:
			source.Eq(field, value)
		case []string:
			if len(value) == 2 {
				switch strings.ToUpper(value[0]) {
				case "EQ":
					source.Eq(field, value[1])
				case "NEQ":
					source.Neq(field, value[1])
				case "GT":
					source.Gt(field, value[1])
				case "EGT":
					source.Egt(field, value[1])
				case "LT":
					source.Lt(field, value[1])
				case "ELT":
					source.Elt(field, value[1])
				case "LIKE":
					source.Like(field, value[1])
				case "NOT LIKE":
					source.NotLike(field, value[1])
				case "IN":
					source.In(field, value[1])
				case "NOT IN":
					source.NotIn(field, value[1])
				}
			}

		}
	}

	return source
}

/*Eq查询条件*/
func (source *Source) Eq(k string, v interface{}) *Source {
	source.where(k, v, "=")
	return source
}

/*NEq查询条件*/
func (source *Source) Neq(k string, v interface{}) *Source {
	source.where(k, v, "<>")
	return source
}

/*Gt查询条件*/
func (source *Source) Gt(k string, v interface{}) *Source {
	source.where(k, v, ">")
	return source
}

/*EGt查询条件*/
func (source *Source) Egt(k string, v interface{}) *Source {
	source.where(k, v, ">=")
	return source
}

/*Lt查询条件*/
func (source *Source) Lt(k string, v interface{}) *Source {
	source.where(k, v, "<")
	return source
}

/*ELt查询条件*/
func (source *Source) Elt(k string, v interface{}) *Source {
	source.where(k, v, "<=")
	return source
}

/*Like查询条件*/
func (source *Source) Like(k string, v string) *Source {
	source.where(k, string(v), "LIKE")
	return source
}

/*Not Like查询条件*/
func (source *Source) NotLike(k string, v string) *Source {
	source.where(k, string(v), "NOT LIKE")
	return source
}

/**
 * In查询条件
 * 参数 k 参数名称
 * 参数 v 查询条件值, 如 "1,2,3,4" 或 []string{"1", "2", "3", "4"}
 */
func (source *Source) In(k string, v interface{}) *Source {
	source.in(k, v, "IN")
	return source
}

/**
 * NOT IN查询条件
 * 参考 In()
 */
func (source *Source) NotIn(k string, v interface{}) *Source {
	source.in(k, v, "NOT IN")
	return source
}

/**
 * 自定义SQL语句
 * 参数 sql 要查询的SQL语句
 * 参数 args 查询参数
 */
func (source *Source) WhereSql(sql string, args ...interface{}) *Source {
	if sql != "" {
		source.wheres = append(source.wheres, sql)
		source.args = append(source.args, args...)
	}

	return source
}

/**
 * 查询分组
 * 参数 fields 分组字段
 */
func (source *Source) Group(fields string) *Source {
	source.group = fields
	return source
}

/**
 * 分组查询条件
 * 参数 having 分组条件
 * 参数 args   条件参数
 */
func (source *Source) Having(having string, args ...interface{}) *Source {
	if having != "" {
		source.having = having
		source.args = append(source.args, args...)
	}

	return source
}

/**
 * 查询条数限制
 * 参数 limit 限制条数
 * 参数 offset 偏移量
 */
func (source *Source) Limit(limit int64) *Source {
	if limit > 0 {
		source.limit = limit
	}

	return source
}

/**
 * 查询偏移量
 * 参数 offset 偏移量
 */
func (source *Source) Offset(offset int64) *Source {
	source.offset = offset
	return source
}

/**
 * 升序
 * 参数 colums 要排序的字段 "age,type"
 */
func (source *Source) Asc(columns string) *Source {
	source.order(columns, "ASC")
	return source
}

/**
 * 降序
 */
func (source *Source) Desc(columns string) *Source {
	source.order(columns, "DESC")
	return source
}

/**
 * 排序
 * 参数 fields 要排序的字段
 * 参数 mode 排序方式 ASC OR DESC
 */
func (source *Source) order(fields string, mode string) *Source {
	var sort []string

	for _, field := range strings.Split(fields, ",") {
		sort = append(sort, fmt.Sprintf("`%s` %s", strings.Join(strings.Split(field, "."), "`.`"), mode))
	}

	if len(sort) > 0 {
		source.orders = append(source.orders, sort...)
	}

	return source
}

/**
 * where 查询处理 (私有方法)
 * 参数 k 字段名称
 * 参数 v 条件值 数字或字符串
 * 参数 条件符号
 */
func (source *Source) where(k string, v interface{}, s string) {
	source.wheres = append(source.wheres, fmt.Sprintf("`%s` %s ?", strings.Join(strings.Split(k, "."), "`.`"), s))
	source.args = append(source.args, v)
}

/**
 * in 查询处理 (私有方法)
 */
func (source *Source) in(k string, in interface{}, symbol string) {
	var set []interface{}

	switch value := in.(type) {
	case string:
		for _, v := range strings.Split(value, ",") {
			set = append(set, v)
		}
	case []interface{}:
		for _, v := range value {
			set = append(set, v)
		}
	}

	/*有效参数*/
	if len(set) < 1 {
		source.args = append(source.args, set...)
		source.wheres = append(source.wheres, fmt.Sprintf("`%s` %s (%s)", strings.Join(strings.Split(k, "."), "`.`"), symbol, strings.TrimRight(strings.Repeat("?,", len(set)), ",")))
	}
}

/**
 * 编译查询
 */
func (source *Source) compileSelectSql() (string, error) {
	sqlstr := "SELECT "

	if len(source.fields) < 1 {
		return "", errors.New("select field cannot be empty")
	}

	/*查询字段*/
	sqlstr += strings.Join(source.fields, ",")

	if source.table == "" {
		return "", errors.New("table cannot be empty")
	}

	sqlstr += fmt.Sprintf(" FROM `%s`", source.fullTable())

	/*表的别名*/
	if source.alias != "" {
		sqlstr += fmt.Sprintf(" `%s`", source.alias)
	}

	/*JOIN*/
	if len(source.joins) > 0 {
		sqlstr += " " + strings.Join(source.joins, " ")
	}

	/*WHERE*/
	sqlstr += " WHERE "

	if len(source.wheres) > 0 {
		sqlstr += strings.Join(source.wheres, " AND ")
	} else {
		sqlstr += "1=1"
	}

	/*GROUP BY*/
	if source.group != "" {
		sqlstr += " GROUP BY " + source.group

		/*HAVING*/
		if source.group != "" {
			sqlstr += " HAVING " + source.having
		}
	}

	/*ORDER BY*/
	if len(source.orders) > 0 {
		sqlstr += " ORDER BY " + strings.Join(source.orders, ",")
	}

	/*LIMIT OFFSET*/
	sqlstr += " LIMIT "

	if source.limit > 0 {
		sqlstr += fmt.Sprintf("%d OFFSET %d", source.limit, source.offset)
	} else {
		sqlstr += fmt.Sprintf("%d,100", source.offset)
	}

	/*记录SQL*/
	if source.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), source.args...))
	}

	return sqlstr, nil
}

/**
 * 编译更新条件
 */
func (source *Source) compileUpdateCondition() string {
	if len(source.wheres) > 0 {
		return " WHERE " + strings.Join(source.wheres, " AND ")
	}

	return ""
}

/**
 * 编译删除条件
 */
func (source *Source) compileDeleteCondition() string {
	compile := " WHERE "

	if len(source.wheres) > 0 {
		compile += strings.Join(source.wheres, " AND ")
	} else {
		compile += "1=1"
	}

	/*ORDER BY*/
	if len(source.orders) > 0 {
		compile += " ORDER BY " + strings.Join(source.orders, ",")
	}

	/*LIMIT*/
	if source.limit > 0 {
		compile += fmt.Sprintf(" LIMIT %d", source.limit)
	}

	return compile
}

/**
 * 编译INSERT语句
 */
func (source *Source) compileInsertSql(data map[string]interface{}) (string, error) {
	if len(data) < 1 {
		return "", errors.New("cannot insert empty data")
	}

	if source.table == "" {
		return "", errors.New("table cannot be empty")
	}

	var keys, values []string

	for key, value := range data {
		keys = append(keys, "`"+key+"`")
		values = append(values, "?")
		/*填充参数*/
		source.args = append(source.args, value)
	}

	sqlstr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUE (%s)", source.fullTable(), strings.Join(keys, ","), strings.Join(values, ","))

	/*记录SQL*/
	if source.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), source.args...))
	}

	return sqlstr, nil
}

/**
 * 编译Update语句
 */
func (source *Source) compileUpdateSql(data map[string]interface{}) (string, error) {
	if len(data) < 1 {
		return "", errors.New("cannot update empty data")
	}

	if source.table == "" {
		return "", errors.New("table cannot be empty")
	}

	var set []string
	var args []interface{}

	for key, value := range data {
		if v, ok := value.(Value); ok {
			set = append(set, fmt.Sprintf("`%s` = %s", key, v.sql))
			continue
		}

		set = append(set, fmt.Sprintf("`%s` = ?", key))
		args = append(args, value)
	}

	/*合并参数*/
	source.args = append(args, source.args...)

	sqlstr := fmt.Sprintf("UPDATE `%s` SET %s%s", source.fullTable(), strings.Join(set, ","), source.compileUpdateCondition())

	/*记录SQL*/
	if source.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), source.args...))
	}

	return sqlstr, nil
}

/**
 * 编译Delete语句
 */
func (source *Source) compileDeleteSql() (string, error) {
	if source.table == "" {
		return "", errors.New("table cannot be empty")
	}

	sqlstr := fmt.Sprintf("DELETE FROM `%s`%s", source.fullTable(), source.compileDeleteCondition())

	/*记录SQL*/
	if source.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), source.args...))
	}

	return sqlstr, nil
}

/**
 * 重置查询
 */
func (source *Source) reset() {
	source.prefix = ""
	source.table = ""
	source.alias = ""
	source.group = ""
	source.having = ""
	source.limit = 0
	source.offset = 0
	source.record = false
	source.fields = []string{}
	source.wheres = []string{}
	source.joins = []string{}
	source.orders = []string{}
	source.args = []interface{}{}
}
