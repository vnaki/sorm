package sorm

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
type Sorm struct {
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
var sorm *Sorm

/**
 * 数据库资源
 */
func NewOrm(dsn string) *Sorm {
	if sorm == nil {
		sorm = &sorm{dsn: dsn}
		sorm.connect()
	}

	return sorm
}

/**
 * 判断是否已实例化
 */
func IsInstantiated() bool {
	return sorm != nil
}

/**
 * 关闭数据库连接
 */
func Close() {
	if sorm != nil {
		sorm.Close()
	}
}

/*连接数据库*/
func (sorm *Sorm) connect() {
	db, err := sql.Open("mysql", sorm.dsn)

	if err != nil {
		panic(err)
	}

	/*最大空闲连接数*/
	db.SetMaxIdleConns(5)
	/*保存句柄*/
	sorm.db = db
}

/*关闭数据库连接*/
func (sorm *Sorm) Close() {
	if err := sorm.db.Close(); err != nil {
		panic(err)
	}
}

/*
 * 表前缀
 */
func (sorm *Sorm) Prefix(prefix string) *Sorm {
	sorm.prefix = prefix
	return sorm
}

/*
 * 指定表名称
 * 参数 table 表的名称
 * 参数 alias 表的别名
 */
func (sorm *Sorm) Table(table string) *Sorm {
	sorm.table = table
	return sorm
}

/*
 * 记录SQL语句
 */
func (sorm *Sorm) Record() *Sorm {
	sorm.record = true
	return sorm
}

/**
 * 完整表名
 */
func (sorm *Sorm) fullTable() string {
	return sorm.prefix + sorm.table
}

/*
 * 表的别名
 */
func (sorm *Sorm) Alias(alias string) *Sorm {
	sorm.alias = alias
	return sorm
}

/**
 * 要查询的字段(字段已处理)
 */
func (sorm *Sorm) Fields(fields string) *Sorm {
	if fields != "" {
		if "*" != fields {
			for _, field := range strings.Split(fields, ",") {
				sorm.fields = append(sorm.fields, fmt.Sprintf("`%s`", strings.Join(strings.Split(field, "."), "`.`")))
			}
		} else {
			sorm.fields = []string{"*"}
		}
	}

	return sorm
}

/**
 * 要查询的字段(字段未处理)
 */
func (sorm *Sorm) RawFields(fields string) *Sorm {
	if fields != "" {
		sorm.fields = strings.Split(fields, ",")
	}

	return sorm
}

/**
 * 查询一条数据
 */
func (sorm *Sorm) One() (map[string]string, error) {

	/*只取一条数据*/
	if sorm.limit == 0 {
		sorm.limit = 1
	}

	data, err := sorm.All()

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
func (sorm *Sorm) All() ([]map[string]string, error) {

	defer sorm.reset()

	sqlstr, err := sorm.compileSelectSql()

	/*错误*/
	if err != nil {
		return []map[string]string{}, err
	}

	/*查询预处理*/
	stmt, err := sorm.db.Prepare(sqlstr)

	/*错误*/
	if err != nil {
		return []map[string]string{}, err
	}

	rows, err := stmt.Query(sorm.args...)

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
func (sorm *Sorm) Count() (int64, error) {
	var field string = "COUNT(*)"

	result, err := sorm.RawFields(field).One()

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
func (sorm *Sorm) Sum(fields string) (map[string]int64, error) {
	return sorm.aggregate(fields, "SUM")
}

/**
 * 求最大值
 * ```
 * Max("field,...")
 * ```
 */
func (sorm *Sorm) Max(fields string) (map[string]int64, error) {
	return sorm.aggregate(fields, "MAX")
}

/**
 * 求最小值
 * ```
 * Min("field,...")
 * ```
 */
func (sorm *Sorm) Min(fields string) (map[string]int64, error) {
	return sorm.aggregate(fields, "MIN")
}

/**
 * 求平均值
 * ```
 * Avg("field,...")
 * ```
 */
func (sorm *Sorm) Avg(fields string) (map[string]int64, error) {
	return sorm.aggregate(fields, "AVG")
}

/**
 * 聚合查询
 * 参数 fields 聚合字段
 * 参数 mode   函数模式 SUM/MAX/MIN/AVG
 */
func (sorm *Sorm) aggregate(fields string, mode string) (map[string]int64, error) {
	/*切割字段*/
	splitFields := strings.Split(fields, ",")
	copyFields := []string{}

	for index, field := range splitFields {
		copyFields = append(copyFields, field)
		splitFields[index] = fmt.Sprintf("%s(`%s`)", mode, field)
	}

	sum, err := sorm.RawFields(strings.Join(splitFields, ",")).One()

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
func (sorm *Sorm) Insert(data map[string]interface{}) (int64, error) {
	/*释放参数*/
	defer sorm.reset()

	/*SQL编译*/
	sqlstr, err := sorm.compileInsertSql(data)

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := sorm.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(sorm.args...)

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
func (sorm *Sorm) Update(data map[string]interface{}) (int64, error) {
	/*释放参数*/
	defer sorm.reset()

	/*SQL编译*/
	sqlstr, err := sorm.compileUpdateSql(data)

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := sorm.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(sorm.args...)

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
func (sorm *Sorm) UpdateFiled(name string, value interface{}) (int64, error) {
	return sorm.Update(map[string]interface{}{name: value})
}

/**
 * 值加法更新 (可以同时更新多个字段值)
 */
func (sorm *Sorm) Increase(data map[string]uint64) (int64, error) {
	values := map[string]interface{}{}

	for key, value := range data {
		values[key] = RawValue(fmt.Sprintf("`%s`+%d", key, value))
	}

	return sorm.Update(values)
}

/**
 * 值减法更新 (可以同时更新多个字段值)
 */
func (sorm *Sorm) Decrease(data map[string]uint64) (int64, error) {
	values := map[string]interface{}{}

	for key, value := range data {
		values[key] = RawValue(fmt.Sprintf("`%s`-%d", key, value))
	}

	return sorm.Update(values)
}

/*删除数据*/
func (sorm *Sorm) Delete() (int64, error) {
	/*释放参数*/
	defer sorm.reset()

	/*SQL编译*/
	sqlstr, err := sorm.compileDeleteSql()

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := sorm.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(sorm.args...)

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
func (sorm *Sorm) Inner(table string, on string) *Sorm {
	sorm.join(table, on, "INNER")
	return sorm
}

/*LEFT JOIN语句*/
func (sorm *Sorm) Left(table string, on string) *Sorm {
	sorm.join(table, on, "LEFT")
	return sorm
}

/*RIGHT JOIN语句*/
func (sorm *Sorm) Right(table string, on string) *Sorm {
	sorm.join(table, on, "RIGHT")
	return sorm
}

/*JOIN语句*/
func (sorm *Sorm) join(table string, on string, mode string) {
	sorm.joins = append(sorm.joins, fmt.Sprintf("%s JOIN %s ON %s", mode, table, on))
}

/*数组查询条件*/
func (sorm *Sorm) Where(where map[string]interface{}) *Sorm {
	for field, unknown := range where {
		switch value := unknown.(type) {
		case string:
			sorm.Eq(field, value)
		case int:
			sorm.Eq(field, value)
		case int8:
			sorm.Eq(field, value)
		case int16:
			sorm.Eq(field, value)
		case int32:
			sorm.Eq(field, value)
		case int64:
			sorm.Eq(field, value)
		case []string:
			if len(value) == 2 {
				switch strings.ToUpper(value[0]) {
				case "EQ":
					sorm.Eq(field, value[1])
				case "NEQ":
					sorm.Neq(field, value[1])
				case "GT":
					sorm.Gt(field, value[1])
				case "EGT":
					sorm.Egt(field, value[1])
				case "LT":
					sorm.Lt(field, value[1])
				case "ELT":
					sorm.Elt(field, value[1])
				case "LIKE":
					sorm.Like(field, value[1])
				case "NOT LIKE":
					sorm.NotLike(field, value[1])
				case "IN":
					sorm.In(field, value[1])
				case "NOT IN":
					sorm.NotIn(field, value[1])
				}
			}

		}
	}

	return sorm
}

/*Eq查询条件*/
func (sorm *Sorm) Eq(k string, v interface{}) *Sorm {
	sorm.where(k, v, "=")
	return sorm
}

/*NEq查询条件*/
func (sorm *Sorm) Neq(k string, v interface{}) *Sorm {
	sorm.where(k, v, "<>")
	return sorm
}

/*Gt查询条件*/
func (sorm *Sorm) Gt(k string, v interface{}) *Sorm {
	sorm.where(k, v, ">")
	return sorm
}

/*EGt查询条件*/
func (sorm *Sorm) Egt(k string, v interface{}) *Sorm {
	sorm.where(k, v, ">=")
	return sorm
}

/*Lt查询条件*/
func (sorm *Sorm) Lt(k string, v interface{}) *Sorm {
	sorm.where(k, v, "<")
	return sorm
}

/*ELt查询条件*/
func (sorm *Sorm) Elt(k string, v interface{}) *Sorm {
	sorm.where(k, v, "<=")
	return sorm
}

/*Like查询条件*/
func (sorm *Sorm) Like(k string, v string) *Sorm {
	sorm.where(k, string(v), "LIKE")
	return sorm
}

/*Not Like查询条件*/
func (sorm *Sorm) NotLike(k string, v string) *Sorm {
	sorm.where(k, string(v), "NOT LIKE")
	return sorm
}

/**
 * In查询条件
 * 参数 k 参数名称
 * 参数 v 查询条件值, 如 "1,2,3,4" 或 []string{"1", "2", "3", "4"}
 */
func (sorm *Sorm) In(k string, v interface{}) *Sorm {
	sorm.in(k, v, "IN")
	return sorm
}

/**
 * NOT IN查询条件
 * 参考 In()
 */
func (sorm *Sorm) NotIn(k string, v interface{}) *Sorm {
	sorm.in(k, v, "NOT IN")
	return sorm
}

/**
 * 自定义SQL语句
 * 参数 sql 要查询的SQL语句
 * 参数 args 查询参数
 */
func (sorm *Sorm) WhereSql(sql string, args ...interface{}) *Sorm {
	if sql != "" {
		sorm.wheres = append(sorm.wheres, sql)
		sorm.args = append(sorm.args, args...)
	}

	return sorm
}

/**
 * 查询分组
 * 参数 fields 分组字段
 */
func (sorm *Sorm) Group(fields string) *Sorm {
	sorm.group = fields
	return sorm
}

/**
 * 分组查询条件
 * 参数 having 分组条件
 * 参数 args   条件参数
 */
func (sorm *Sorm) Having(having string, args ...interface{}) *Sorm {
	if having != "" {
		sorm.having = having
		sorm.args = append(sorm.args, args...)
	}

	return sorm
}

/**
 * 查询条数限制
 * 参数 limit 限制条数
 * 参数 offset 偏移量
 */
func (sorm *Sorm) Limit(limit int64) *Sorm {
	if limit > 0 {
		sorm.limit = limit
	}

	return sorm
}

/**
 * 查询偏移量
 * 参数 offset 偏移量
 */
func (sorm *Sorm) Offset(offset int64) *Sorm {
	sorm.offset = offset
	return sorm
}

/**
 * 升序
 * 参数 colums 要排序的字段 "age,type"
 */
func (sorm *Sorm) Asc(columns string) *Sorm {
	sorm.order(columns, "ASC")
	return sorm
}

/**
 * 降序
 */
func (sorm *Sorm) Desc(columns string) *Sorm {
	sorm.order(columns, "DESC")
	return sorm
}

/**
 * 排序
 * 参数 fields 要排序的字段
 * 参数 mode 排序方式 ASC OR DESC
 */
func (sorm *Sorm) order(fields string, mode string) *Sorm {
	var sort []string

	for _, field := range strings.Split(fields, ",") {
		sort = append(sort, fmt.Sprintf("`%s` %s", strings.Join(strings.Split(field, "."), "`.`"), mode))
	}

	if len(sort) > 0 {
		sorm.orders = append(sorm.orders, sort...)
	}

	return sorm
}

/**
 * where 查询处理 (私有方法)
 * 参数 k 字段名称
 * 参数 v 条件值 数字或字符串
 * 参数 条件符号
 */
func (sorm *Sorm) where(k string, v interface{}, s string) {
	sorm.wheres = append(sorm.wheres, fmt.Sprintf("`%s` %s ?", strings.Join(strings.Split(k, "."), "`.`"), s))
	sorm.args = append(sorm.args, v)
}

/**
 * in 查询处理 (私有方法)
 */
func (sorm *Sorm) in(k string, in interface{}, symbol string) {
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
		sorm.args = append(sorm.args, set...)
		sorm.wheres = append(sorm.wheres, fmt.Sprintf("`%s` %s (%s)", strings.Join(strings.Split(k, "."), "`.`"), symbol, strings.TrimRight(strings.Repeat("?,", len(set)), ",")))
	}
}

/**
 * 编译查询
 */
func (sorm *Sorm) compileSelectSql() (string, error) {
	sqlstr := "SELECT "

	if len(sorm.fields) < 1 {
		return "", errors.New("select field cannot be empty")
	}

	/*查询字段*/
	sqlstr += strings.Join(sorm.fields, ",")

	if sorm.table == "" {
		return "", errors.New("table cannot be empty")
	}

	sqlstr += fmt.Sprintf(" FROM `%s`", sorm.fullTable())

	/*表的别名*/
	if sorm.alias != "" {
		sqlstr += fmt.Sprintf(" `%s`", sorm.alias)
	}

	/*JOIN*/
	if len(sorm.joins) > 0 {
		sqlstr += " " + strings.Join(sorm.joins, " ")
	}

	/*WHERE*/
	sqlstr += " WHERE "

	if len(sorm.wheres) > 0 {
		sqlstr += strings.Join(sorm.wheres, " AND ")
	} else {
		sqlstr += "1=1"
	}

	/*GROUP BY*/
	if sorm.group != "" {
		sqlstr += " GROUP BY " + sorm.group

		/*HAVING*/
		if sorm.group != "" {
			sqlstr += " HAVING " + sorm.having
		}
	}

	/*ORDER BY*/
	if len(sorm.orders) > 0 {
		sqlstr += " ORDER BY " + strings.Join(sorm.orders, ",")
	}

	/*LIMIT OFFSET*/
	sqlstr += " LIMIT "

	if sorm.limit > 0 {
		sqlstr += fmt.Sprintf("%d OFFSET %d", sorm.limit, sorm.offset)
	} else {
		sqlstr += fmt.Sprintf("%d,100", sorm.offset)
	}

	/*记录SQL*/
	if sorm.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), sorm.args...))
	}

	return sqlstr, nil
}

/**
 * 编译更新条件
 */
func (sorm *Sorm) compileUpdateCondition() string {
	if len(sorm.wheres) > 0 {
		return " WHERE " + strings.Join(sorm.wheres, " AND ")
	}

	return ""
}

/**
 * 编译删除条件
 */
func (sorm *Sorm) compileDeleteCondition() string {
	compile := " WHERE "

	if len(sorm.wheres) > 0 {
		compile += strings.Join(sorm.wheres, " AND ")
	} else {
		compile += "1=1"
	}

	/*ORDER BY*/
	if len(sorm.orders) > 0 {
		compile += " ORDER BY " + strings.Join(sorm.orders, ",")
	}

	/*LIMIT*/
	if sorm.limit > 0 {
		compile += fmt.Sprintf(" LIMIT %d", sorm.limit)
	}

	return compile
}

/**
 * 编译INSERT语句
 */
func (sorm *Sorm) compileInsertSql(data map[string]interface{}) (string, error) {
	if len(data) < 1 {
		return "", errors.New("cannot insert empty data")
	}

	if sorm.table == "" {
		return "", errors.New("table cannot be empty")
	}

	var keys, values []string

	for key, value := range data {
		keys = append(keys, "`"+key+"`")
		values = append(values, "?")
		/*填充参数*/
		sorm.args = append(sorm.args, value)
	}

	sqlstr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUE (%s)", sorm.fullTable(), strings.Join(keys, ","), strings.Join(values, ","))

	/*记录SQL*/
	if sorm.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), sorm.args...))
	}

	return sqlstr, nil
}

/**
 * 编译Update语句
 */
func (sorm *Sorm) compileUpdateSql(data map[string]interface{}) (string, error) {
	if len(data) < 1 {
		return "", errors.New("cannot update empty data")
	}

	if sorm.table == "" {
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
	sorm.args = append(args, sorm.args...)

	sqlstr := fmt.Sprintf("UPDATE `%s` SET %s%s", sorm.fullTable(), strings.Join(set, ","), sorm.compileUpdateCondition())

	/*记录SQL*/
	if sorm.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), sorm.args...))
	}

	return sqlstr, nil
}

/**
 * 编译Delete语句
 */
func (sorm *Sorm) compileDeleteSql() (string, error) {
	if sorm.table == "" {
		return "", errors.New("table cannot be empty")
	}

	sqlstr := fmt.Sprintf("DELETE FROM `%s`%s", sorm.fullTable(), sorm.compileDeleteCondition())

	/*记录SQL*/
	if sorm.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), sorm.args...))
	}

	return sqlstr, nil
}

/**
 * 重置查询
 */
func (sorm *Sorm) reset() {
	sorm.prefix = ""
	sorm.table = ""
	sorm.alias = ""
	sorm.group = ""
	sorm.having = ""
	sorm.limit = 0
	sorm.offset = 0
	sorm.record = false
	sorm.fields = []string{}
	sorm.wheres = []string{}
	sorm.joins = []string{}
	sorm.orders = []string{}
	sorm.args = []interface{}{}
}
