### 1, 配置

1. 配置文件 

   ```
   sqoop-env.sh 几个路径
   ```

2. jdbc `cp /usr/local/apache-hive-3.1.1-bin/lib/mysql-connector-java-8.0.14.jar lib`
3. 启动 sqoop help
4. 连接数据库   `sqoop list-databases --connect jdbc:mysql://mysql1:3306 --username hive --password hive`

### 案例

```sql
create database company;
create table staff(
							id int(4) primary key not null auto_increment, 
							name varchar(255) not null, 
							sex varchar(255) not null);
insert into staff(name, sex) values('Thomas', 'Male');  
```

#### 1, 使用sqoop 将mysql 中的数据导入到hdfs

全部导入

```bash
sqoop import --connect jdbc:mysql://mysql1:3306/company \
--username hive \
--password hive \
--table staff \
--target-dir /user/company \
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by "\t"
```

查询导入

```bash
sqoop import \
--connect jdbc:mysql://mysql1:3306/company \
--username hive \
-P \
--target-dir /user/company \
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by "\t" \
--query 'select name, sex from staff where id >= 2 and $CONDITIONS'
```

导入指定列

```bash
sqoop import \
--connect jdbc:mysql://mysql1:3306/company \
--username hive \
-P \
--target-dir /user/company \
--table staff \
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by "\t" \
--columns "id, sex"


```

使用sqoop 关键字筛选查询导入数据 ( 把query 拆开来写)

```bash
sqoop import \
--connect jdbc:mysql://mysql1:3306/company \
--username hive \
-P \
--target-dir /user/company \
--delete-target-dir  \
--num-mappers 1 \
--fields-terminated-by "\t" \
--table staff \
--where "id=3"




```

#### sqoop hive

向hive 中导入数据 (先拷贝到hdfs 上面, 再导入hive), 如果不写路径 默认 `/user/用户名`

```shell
sqoop import \
--connect jdbc:mysql://mysql1:3306/company \
--username hive \
-P \
--delete-target-dir  \
--table staff \
--num-mappers 1 \
--hive-import  \
--fields-terminated-by "\t" \
--hive-overwrite  \
--hive-table company.staff_hive1
```

hive /hdfs ----> mysql

在mysql 建立表

`create table staff_mysql(
id int(4) primary key not null auto_increment, 
name varchar(255) not null, 
sex varchar(255) not null);`

```shell
sqoop export  \
--connect jdbc:mysql://mysql1:3306/company \
--username hive \
-P  \
--table staff_mysql \
--num-mappers 1  \
--export-dir /user/hive/warehouse/company.db/staff_hive1 \
--input-fields-terminated-by "\t" 
```

使用opt 文件打包 sqoop 命令























