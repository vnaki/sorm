package orm

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
type Orm struct {
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
var orm *Orm

/**
 * 数据库资源
 */
func Intance(dsn string) *Orm {
	if orm == nil {
		orm = &Orm{dsn: dsn}
		orm.connect()
	}

	return orm
}

/**
 * 判断是否已实例化
 */
func IsInstantiated() bool {
	return orm != nil
}

/**
 * 关闭数据库连接
 */
func Close() {
	if orm != nil {
		orm.Close()
	}
}

/*连接数据库*/
func (orm *Orm) connect() {
	db, err := sql.Open("mysql", orm.dsn)

	if err != nil {
		panic(err)
	}

	/*最大空闲连接数*/
	db.SetMaxIdleConns(5)
	/*保存句柄*/
	orm.db = db
}

/*关闭数据库连接*/
func (orm *Orm) Close() {
	if err := orm.db.Close(); err != nil {
		panic(err)
	}
}

/*
 * 表前缀
 */
func (orm *Orm) Prefix(prefix string) *Orm {
	orm.prefix = prefix
	return orm
}

/*
 * 指定表名称
 * 参数 table 表的名称
 * 参数 alias 表的别名
 */
func (orm *Orm) Table(table string) *Orm {
	orm.table = table
	return orm
}

/*
 * 记录SQL语句
 */
func (orm *Orm) Record() *Orm {
	orm.record = true
	return orm
}

/**
 * 完整表名
 */
func (orm *Orm) fullTable() string {
	return orm.prefix + orm.table
}

/*
 * 表的别名
 */
func (orm *Orm) Alias(alias string) *Orm {
	orm.alias = alias
	return orm
}

/**
 * 要查询的字段(字段已处理)
 */
func (orm *Orm) Fields(fields string) *Orm {
	if fields != "" {
		if "*" != fields {
			for _, field := range strings.Split(fields, ",") {
				orm.fields = append(orm.fields, fmt.Sprintf("`%s`", strings.Join(strings.Split(field, "."), "`.`")))
			}
		} else {
			orm.fields = []string{"*"}
		}
	}

	return orm
}

/**
 * 要查询的字段(字段未处理)
 */
func (orm *Orm) RawFields(fields string) *Orm {
	if fields != "" {
		orm.fields = strings.Split(fields, ",")
	}

	return orm
}

/**
 * 查询一条数据
 */
func (orm *Orm) One() (map[string]string, error) {

	/*只取一条数据*/
	if orm.limit == 0 {
		orm.limit = 1
	}

	data, err := orm.All()

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
func (orm *Orm) All() ([]map[string]string, error) {

	defer orm.reset()

	sqlstr, err := orm.compileSelectSql()

	/*错误*/
	if err != nil {
		return []map[string]string{}, err
	}

	/*查询预处理*/
	stmt, err := orm.db.Prepare(sqlstr)

	/*错误*/
	if err != nil {
		return []map[string]string{}, err
	}

	rows, err := stmt.Query(orm.args...)

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
func (orm *Orm) Count() (int64, error) {
	var field string = "COUNT(*)"

	result, err := orm.RawFields(field).One()

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
func (orm *Orm) Sum(fields string) (map[string]int64, error) {
	return orm.aggregate(fields, "SUM")
}

/**
 * 求最大值
 * ```
 * Max("field,...")
 * ```
 */
func (orm *Orm) Max(fields string) (map[string]int64, error) {
	return orm.aggregate(fields, "MAX")
}

/**
 * 求最小值
 * ```
 * Min("field,...")
 * ```
 */
func (orm *Orm) Min(fields string) (map[string]int64, error) {
	return orm.aggregate(fields, "MIN")
}

/**
 * 求平均值
 * ```
 * Avg("field,...")
 * ```
 */
func (orm *Orm) Avg(fields string) (map[string]int64, error) {
	return orm.aggregate(fields, "AVG")
}

/**
 * 聚合查询
 * 参数 fields 聚合字段
 * 参数 mode   函数模式 SUM/MAX/MIN/AVG
 */
func (orm *Orm) aggregate(fields string, mode string) (map[string]int64, error) {
	/*切割字段*/
	splitFields := strings.Split(fields, ",")
	copyFields := []string{}

	for index, field := range splitFields {
		copyFields = append(copyFields, field)
		splitFields[index] = fmt.Sprintf("%s(`%s`)", mode, field)
	}

	sum, err := orm.RawFields(strings.Join(splitFields, ",")).One()

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
func (orm *Orm) Insert(data map[string]interface{}) (int64, error) {
	/*释放参数*/
	defer orm.reset()

	/*SQL编译*/
	sqlstr, err := orm.compileInsertSql(data)

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := orm.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(orm.args...)

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
func (orm *Orm) Update(data map[string]interface{}) (int64, error) {
	/*释放参数*/
	defer orm.reset()

	/*SQL编译*/
	sqlstr, err := orm.compileUpdateSql(data)

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := orm.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(orm.args...)

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
func (orm *Orm) UpdateFiled(name string, value interface{}) (int64, error) {
	return orm.Update(map[string]interface{}{name: value})
}

/**
 * 值加法更新 (可以同时更新多个字段值)
 */
func (orm *Orm) Increase(data map[string]uint64) (int64, error) {
	values := map[string]interface{}{}

	for key, value := range data {
		values[key] = RawValue(fmt.Sprintf("`%s`+%d", key, value))
	}

	return orm.Update(values)
}

/**
 * 值减法更新 (可以同时更新多个字段值)
 */
func (orm *Orm) Decrease(data map[string]uint64) (int64, error) {
	values := map[string]interface{}{}

	for key, value := range data {
		values[key] = RawValue(fmt.Sprintf("`%s`-%d", key, value))
	}

	return orm.Update(values)
}

/*删除数据*/
func (orm *Orm) Delete() (int64, error) {
	/*释放参数*/
	defer orm.reset()

	/*SQL编译*/
	sqlstr, err := orm.compileDeleteSql()

	/*编译SQL语句错误*/
	if err != nil {
		return 0, err
	}

	/*SQL预编译处理*/
	stmt, err := orm.db.Prepare(sqlstr)

	if err != nil {
		return 0, err
	}

	result, err := stmt.Exec(orm.args...)

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
func (orm *Orm) Inner(table string, on string) *Orm {
	orm.join(table, on, "INNER")
	return orm
}

/*LEFT JOIN语句*/
func (orm *Orm) Left(table string, on string) *Orm {
	orm.join(table, on, "LEFT")
	return orm
}

/*RIGHT JOIN语句*/
func (orm *Orm) Right(table string, on string) *Orm {
	orm.join(table, on, "RIGHT")
	return orm
}

/*JOIN语句*/
func (orm *Orm) join(table string, on string, mode string) {
	orm.joins = append(orm.joins, fmt.Sprintf("%s JOIN %s ON %s", mode, table, on))
}

/*数组查询条件*/
func (orm *Orm) Where(where map[string]interface{}) *Orm {
	for field, unknown := range where {
		switch value := unknown.(type) {
		case string:
			orm.Eq(field, value)
		case int:
			orm.Eq(field, value)
		case int8:
			orm.Eq(field, value)
		case int16:
			orm.Eq(field, value)
		case int32:
			orm.Eq(field, value)
		case int64:
			orm.Eq(field, value)
		case []string:
			if len(value) == 2 {
				switch strings.ToUpper(value[0]) {
				case "EQ":
					orm.Eq(field, value[1])
				case "NEQ":
					orm.Neq(field, value[1])
				case "GT":
					orm.Gt(field, value[1])
				case "EGT":
					orm.Egt(field, value[1])
				case "LT":
					orm.Lt(field, value[1])
				case "ELT":
					orm.Elt(field, value[1])
				case "LIKE":
					orm.Like(field, value[1])
				case "NOT LIKE":
					orm.NotLike(field, value[1])
				case "IN":
					orm.In(field, value[1])
				case "NOT IN":
					orm.NotIn(field, value[1])
				}
			}

		}
	}

	return orm
}

/*Eq查询条件*/
func (orm *Orm) Eq(k string, v interface{}) *Orm {
	orm.where(k, v, "=")
	return orm
}

/*NEq查询条件*/
func (orm *Orm) Neq(k string, v interface{}) *Orm {
	orm.where(k, v, "<>")
	return orm
}

/*Gt查询条件*/
func (orm *Orm) Gt(k string, v interface{}) *Orm {
	orm.where(k, v, ">")
	return orm
}

/*EGt查询条件*/
func (orm *Orm) Egt(k string, v interface{}) *Orm {
	orm.where(k, v, ">=")
	return orm
}

/*Lt查询条件*/
func (orm *Orm) Lt(k string, v interface{}) *Orm {
	orm.where(k, v, "<")
	return orm
}

/*ELt查询条件*/
func (orm *Orm) Elt(k string, v interface{}) *Orm {
	orm.where(k, v, "<=")
	return orm
}

/*Like查询条件*/
func (orm *Orm) Like(k string, v string) *Orm {
	orm.where(k, string(v), "LIKE")
	return orm
}

/*Not Like查询条件*/
func (orm *Orm) NotLike(k string, v string) *Orm {
	orm.where(k, string(v), "NOT LIKE")
	return orm
}

/**
 * In查询条件
 * 参数 k 参数名称
 * 参数 v 查询条件值, 如 "1,2,3,4" 或 []string{"1", "2", "3", "4"}
 */
func (orm *Orm) In(k string, v interface{}) *Orm {
	orm.in(k, v, "IN")
	return orm
}

/**
 * NOT IN查询条件
 * 参考 In()
 */
func (orm *Orm) NotIn(k string, v interface{}) *Orm {
	orm.in(k, v, "NOT IN")
	return orm
}

/**
 * 自定义SQL语句
 * 参数 sql 要查询的SQL语句
 * 参数 args 查询参数
 */
func (orm *Orm) WhereSql(sql string, args ...interface{}) *Orm {
	if sql != "" {
		orm.wheres = append(orm.wheres, sql)
		orm.args = append(orm.args, args...)
	}

	return orm
}

/**
 * 查询分组
 * 参数 fields 分组字段
 */
func (orm *Orm) Group(fields string) *Orm {
	orm.group = fields
	return orm
}

/**
 * 分组查询条件
 * 参数 having 分组条件
 * 参数 args   条件参数
 */
func (orm *Orm) Having(having string, args ...interface{}) *Orm {
	if having != "" {
		orm.having = having
		orm.args = append(orm.args, args...)
	}

	return orm
}

/**
 * 查询条数限制
 * 参数 limit 限制条数
 * 参数 offset 偏移量
 */
func (orm *Orm) Limit(limit int64) *Orm {
	if limit > 0 {
		orm.limit = limit
	}

	return orm
}

/**
 * 查询偏移量
 * 参数 offset 偏移量
 */
func (orm *Orm) Offset(offset int64) *Orm {
	orm.offset = offset
	return orm
}

/**
 * 升序
 * 参数 colums 要排序的字段 "age,type"
 */
func (orm *Orm) Asc(columns string) *Orm {
	orm.order(columns, "ASC")
	return orm
}

/**
 * 降序
 */
func (orm *Orm) Desc(columns string) *Orm {
	orm.order(columns, "DESC")
	return orm
}

/**
 * 排序
 * 参数 fields 要排序的字段
 * 参数 mode 排序方式 ASC OR DESC
 */
func (orm *Orm) order(fields string, mode string) *Orm {
	var sort []string

	for _, field := range strings.Split(fields, ",") {
		sort = append(sort, fmt.Sprintf("`%s` %s", strings.Join(strings.Split(field, "."), "`.`"), mode))
	}

	if len(sort) > 0 {
		orm.orders = append(orm.orders, sort...)
	}

	return orm
}

/**
 * where 查询处理 (私有方法)
 * 参数 k 字段名称
 * 参数 v 条件值 数字或字符串
 * 参数 条件符号
 */
func (orm *Orm) where(k string, v interface{}, s string) {
	orm.wheres = append(orm.wheres, fmt.Sprintf("`%s` %s ?", strings.Join(strings.Split(k, "."), "`.`"), s))
	orm.args = append(orm.args, v)
}

/**
 * in 查询处理 (私有方法)
 */
func (orm *Orm) in(k string, in interface{}, symbol string) {
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
		orm.args = append(orm.args, set...)
		orm.wheres = append(orm.wheres, fmt.Sprintf("`%s` %s (%s)", strings.Join(strings.Split(k, "."), "`.`"), symbol, strings.TrimRight(strings.Repeat("?,", len(set)), ",")))
	}
}

/**
 * 编译查询
 */
func (orm *Orm) compileSelectSql() (string, error) {
	sqlstr := "SELECT "

	if len(orm.fields) < 1 {
		return "", errors.New("select field cannot be empty")
	}

	/*查询字段*/
	sqlstr += strings.Join(orm.fields, ",")

	if orm.table == "" {
		return "", errors.New("table cannot be empty")
	}

	sqlstr += fmt.Sprintf(" FROM `%s`", orm.fullTable())

	/*表的别名*/
	if orm.alias != "" {
		sqlstr += fmt.Sprintf(" `%s`", orm.alias)
	}

	/*JOIN*/
	if len(orm.joins) > 0 {
		sqlstr += " " + strings.Join(orm.joins, " ")
	}

	/*WHERE*/
	sqlstr += " WHERE "

	if len(orm.wheres) > 0 {
		sqlstr += strings.Join(orm.wheres, " AND ")
	} else {
		sqlstr += "1=1"
	}

	/*GROUP BY*/
	if orm.group != "" {
		sqlstr += " GROUP BY " + orm.group

		/*HAVING*/
		if orm.group != "" {
			sqlstr += " HAVING " + orm.having
		}
	}

	/*ORDER BY*/
	if len(orm.orders) > 0 {
		sqlstr += " ORDER BY " + strings.Join(orm.orders, ",")
	}

	/*LIMIT OFFSET*/
	sqlstr += " LIMIT "

	if orm.limit > 0 {
		sqlstr += fmt.Sprintf("%d OFFSET %d", orm.limit, orm.offset)
	} else {
		sqlstr += fmt.Sprintf("%d,100", orm.offset)
	}

	/*记录SQL*/
	if orm.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), orm.args...))
	}

	return sqlstr, nil
}

/**
 * 编译更新条件
 */
func (orm *Orm) compileUpdateCondition() string {
	if len(orm.wheres) > 0 {
		return " WHERE " + strings.Join(orm.wheres, " AND ")
	}

	return ""
}

/**
 * 编译删除条件
 */
func (orm *Orm) compileDeleteCondition() string {
	compile := " WHERE "

	if len(orm.wheres) > 0 {
		compile += strings.Join(orm.wheres, " AND ")
	} else {
		compile += "1=1"
	}

	/*ORDER BY*/
	if len(orm.orders) > 0 {
		compile += " ORDER BY " + strings.Join(orm.orders, ",")
	}

	/*LIMIT*/
	if orm.limit > 0 {
		compile += fmt.Sprintf(" LIMIT %d", orm.limit)
	}

	return compile
}

/**
 * 编译INSERT语句
 */
func (orm *Orm) compileInsertSql(data map[string]interface{}) (string, error) {
	if len(data) < 1 {
		return "", errors.New("cannot insert empty data")
	}

	if orm.table == "" {
		return "", errors.New("table cannot be empty")
	}

	var keys, values []string

	for key, value := range data {
		keys = append(keys, "`"+key+"`")
		values = append(values, "?")
		/*填充参数*/
		orm.args = append(orm.args, value)
	}

	sqlstr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUE (%s)", orm.fullTable(), strings.Join(keys, ","), strings.Join(values, ","))

	/*记录SQL*/
	if orm.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), orm.args...))
	}

	return sqlstr, nil
}

/**
 * 编译Update语句
 */
func (orm *Orm) compileUpdateSql(data map[string]interface{}) (string, error) {
	if len(data) < 1 {
		return "", errors.New("cannot update empty data")
	}

	if orm.table == "" {
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
	orm.args = append(args, orm.args...)

	sqlstr := fmt.Sprintf("UPDATE `%s` SET %s%s", orm.fullTable(), strings.Join(set, ","), orm.compileUpdateCondition())

	/*记录SQL*/
	if orm.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), orm.args...))
	}

	return sqlstr, nil
}

/**
 * 编译Delete语句
 */
func (orm *Orm) compileDeleteSql() (string, error) {
	if orm.table == "" {
		return "", errors.New("table cannot be empty")
	}

	sqlstr := fmt.Sprintf("DELETE FROM `%s`%s", orm.fullTable(), orm.compileDeleteCondition())

	/*记录SQL*/
	if orm.record == true {
		UsedSql = append(UsedSql, fmt.Sprintf(strings.Replace(sqlstr, "?", "%v", -1), orm.args...))
	}

	return sqlstr, nil
}

/**
 * 重置查询
 */
func (orm *Orm) reset() {
	orm.prefix = ""
	orm.table = ""
	orm.alias = ""
	orm.group = ""
	orm.having = ""
	orm.limit = 0
	orm.offset = 0
	orm.record = false
	orm.fields = []string{}
	orm.wheres = []string{}
	orm.joins = []string{}
	orm.orders = []string{}
	orm.args = []interface{}{}
}
