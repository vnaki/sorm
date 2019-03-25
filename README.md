##### 使用例子

如果一张用户表User,结构如下:

| 字段名 | 注释 |
| -- | -- |
| id | 用户ID |
| username | 用户名称 |
| mobile | 手机号码 |
| age | 年龄 |
| status | 用户状态 |

- 返回实例

```go
orm := sorm.NewOrm("user:password@tcp(127.0.0.1:3306)/dbname?charset=utf8")
```

- 指定数据表

```go
orm.Prefix("").Table("user")
```

- 查询用户ID为1的信息

```go
orm.Fields("id,username,mobile,age,status").Eq("id", 1).One()
```

- 查询用户ID小于10的用户集,并且按照Id从大到小排序

```go
orm.RawFields("id,username,mobile,age,status").Lt("id", 10).Desc("id").All()
```

- 查询最近加入的前10名用户

```go
orm.Fields("id,username,mobile,age,status").Limit(10).Desc("id").All()
```

- 查询年龄小于30并大于20的用户数量

```go
orm.Lt("age",30).Gt("age",20).Count()
```

- 插入一条数据

```go 
orm.Insert( map[string]interface{} {
  "username" : "meto",
  "mobile"   : "135666777XX",
  "age"      : 20,
  "status"   : 1
})
```

- 更新ID为5的用户名称和年龄

```go 
orm.Where("id", 5).Update( map[string]interface{} {
  "username" : "meye",
  "age"      : 21
})
```

- 更新ID为5的用户的年龄

```go 
orm.Where(map[string]interface{}{"id":5}).UpdateField ("age", 19)
```

- 删除状态为0 且 id大于等于1000的用户 且 年龄小于18

```go
orm.Where(map[string]interface{}{
  "status" : 0,
  "id" : []string{"Egt", 100},
  "age" : []string{"Lt", 18}
}).Delete()
```


##### SORM方法列表

|ORM方法|说明|
|:--|:--|
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
