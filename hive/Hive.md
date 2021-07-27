# Hive

什么是Hive？

优点：

1）采用类SQL语法，快速开发；

2）避免去写MapReduce，减少开发学习成本；

3）Hive延迟比较高，常用语数据分析

4）处理大数据

5）支持用户自定义函数

缺点：

1）HQL表达能力有限

2）Hive效率比较低



### DDL数据定义

#### 创建数据库

```
CREATE DATABASE [IF NOT EXISTS] database_name
[COMMENT database_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES (property_name=property_value, ...)];
```



#### 创建表

```
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name
[(col_name data_type [COMMENT col_comment], ...)]
[COMMENT table_comment]
[PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
[CLUSTERED BY (col_name, col_name, ...)
[SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
[ROW FORMAT row_format]
[STORED AS file_format]
[LOCATION hdfs_path]
[TBLPROPERTIES (property_name=property_value, ...)]
[AS select_statement]
```

案例

test.txt

```txt
songsong,bingbing_lili,xiao song:18_xiaoxiao song:19,hui long guan_beijing
yangyang,caicai_susu,xiao yang:18_xiaoxiao yang:19,chao yang_beijing
```

test.sql

```SQL
create table test(
	name string,
    frineds array<string>,
    children map<string, int>,
    address struct<streeet:string, city:string>
)
row format delimited
fields terminated by ','
collection items termianted by '_'
map keys terminated by ':'
lines terminated by '\n';
```

