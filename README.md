|ORM方法|说明|
|:--|--|
| Prefix(prefix string) *Orm | 设置数据表前缀 |
| Table(table string) *Orm | 设置数据表名称 |
| Alias(alias string) *Orm | 设置数据表的别名 |
| Fields(fields string) *Orm | 指定要查询的字段(多个字段逗号隔开),字段会被标准处理|
|RawFields(fields string) *Orm | 指定要查询的字段(多个字段逗号隔开),字段不会被处理 |
| One() (map[string]string, error | 查询一条数据 |
| All() ([]map[string]string, error) | 返回结果集 |
| Count() (int64, error) | 统计数查询 |
| Sum(fields string) (map[string]int64, error) | 求和 |
| Max(fields string) (map[string]int64, error) | 求最大值 |
| Min(fields string) (map[string]int64, error) | 求最小值 |
| Avg(fields string) (map[string]int64, error) | 求平均值 |
| Insert(data map[string]interface{}) (int64, error) | 插入一条数据 |
| Update(data map[string]interface{}) (int64, error) | 更新数据 |
| UpdateFiled(name string, value interface{}) (int64, error) | 更新制定字段数据 |
| Increase(data map[string]uint64) (int64, error) | 加法运行 |
| Decrease(data map[string]uint64) (int64, error) | 减法运行 |
| Delete() (int64, error) | 删除操作 |
| Inner(table string, on string) *Orm | 内连接查询 INNER JOIN |
| Left(table string, on string) *Orm | 左连接查询 LEFT JOIN |
| Right(table string, on string) *Orm | 右连接查询 LEFT JOIN |
| Where(where map[string]interface{}) *Orm | 条件查询 |
| Eq(k string, v interface{}) *Orm | 等于条件 |
| Neq(k string, v interface{}) *Orm | 不等于条件 |
| Gt(k string, v interface{}) *Orm | 大于条件|
| Egt(k string, v interface{}) *Orm | 大于等于条件 |
| Lt(k string, v interface{}) *Orm | 小于条件 |
| Elt(k string, v interface{}) *Orm | 小于等于 |
| Like(k string, v string) *Orm | LIKE条件 |
| NotLike(k string, v string) *Orm | NOT LIKE 条件 |
| In(k string, v interface{}) *Orm | IN查询 |
| NotIn(k string, v interface{}) *Orm | NOT IN 查询 |
| WhereSql(sql string, args ...interface{}) *Orm | 自定义查询条件SQL |
| Group(fields string) *Orm | 分组查询 |
| Having(having string, args ...interface{}) *Orm | 分组查询条件 |
| Limit(limit int64) *Orm | 查询条数限制 |
| Offset(offset int64) *Orm | 查询偏移量 |
| Asc(columns string) *Orm | 升序 |
| Desc(columns string) *Orm | 降序 |
