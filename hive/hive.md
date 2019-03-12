### hive

数据仓库: hive 是一个客户端, 是MapReduce 的客户端, 不必要每台机器都安装部署hive

hive 特性

1.  操作接口采用SQL 语法,  HQL
2. 避免了写MapReduce 的繁琐过程

#### hive 体系

1. client(终端命令行)
2. metastore (原本数据集和字段名称以及数据信息之间的双射关系)
3. servier hadoop (在操作hive 的同时, 需要将 hadoop 的hdfs 开启, yarn 开启, mapred 配置好)

### 配置 

#### 准备

同一个节点要安装hdfs, mysql 可以是另一个节点上面

#### mysql 8

`CREATE USER 'hive'@'%' IDENTIFIED BY 'hive';`   创建用户

`GRANT ALL PRIVILEGES ON *.* TO 'hive'@'%' ;  更改用户和接触的表

`create database hive`

#### hive 配置

```xml
	<property>
		<name>javax.jdo.option.ConnectionURL</name>
		<value>jdbc:mysql://mysql1:3306/hive?user=root&amp;password=&amp;useUnicode=true&amp;characterEncoding=utf8&amp;autoReconnect=true&amp;createDatabaseIfNotExist=true </value>
		<description>JDBC connect string for a JDBC metastore</description>
	</property>

	<property>
		<name>javax.jdo.option.ConnectionDriverName</name>
		<value>com.mysql.cj.jdbc.Driver</value>
		<description>Driver class name for a JDBC metastore</description>
	</property>

	<property>
		<name>javax.jdo.option.ConnectionUserName</name>
		<value>hive</value>
		<description>username to use against metastore database</description>
	</property>

	<property>
		<name>javax.jdo.option.ConnectionPassword</name>
		<value>hive</value>
		<description>password to use against metastore database</description>
	</property>
	<property>
		<name>hive.cli.print.header</name>
		<value>true</value>
		<description>Whether to print the names of the columns in query output.</description>
	</property>
	<property>
		<name>hive.cli.print.current.db</name>
		<value>true</value>
		<description>Whether to include the current database in the Hive prompt.</description>
	</property>
```

#### log4j

把conf 中 hive-exec-log4j2.properties.template 更名为 hive-exec-log4j2.properties

property.hive.log.dir = /usr/local/apache-hive-3.1.1-bin/logs

#### mysql 连接包

把包放在lib 中

#### 初始化

`schematool -dbType mysql -initSchema`

#### 修改HDFS系统中关于Hive的一些目录权限

​	$ /opt/modules/cdh/hadoop-2.5.0-cdh5.3.6/bin/hadoop fs -chmod 777 /tmp/
​	$ /opt/modules/cdh/hadoop-2.5.0-cdh5.3.6/bin/hadoop fs -chmod 777 /user/hive/warehouse

本地目录: `load data local inpath '文件路径' into table` 

local 表示从本地导入,, 没有就是从 HDFS 

#### hive 的mapreduce 任务

```xml
			<property>
  				<name>hive.fetch.task.conversion</name>
  				<value>more</value>
  				<description>
    			Some select queries can be converted to single FETCH task minimizing latency.
    			Currently the query should be single sourced not having any subquery and should not have
    			any aggregations or distincts (which incurs RS), lateral views and joins.
    			1. minimal : SELECT STAR, FILTER on partition columns, LIMIT only
    			2. more    : SELECT, FILTER, LIMIT only (TABLESAMPLE, virtual columns)
  				</description>
			</property>
```

#### hive 元数据备份还原

备份: mysqldump -uhive -p hive > hive.sql

还原: mysql -uroot -p hive< hive.sql

find 命令: find -name hive.sql

#### Hive 操作HQL 语句的两个参数

一般使用  oozie  azakban    crontab

hive -e " "    hive -f 文件.hql     可以让hive 执行hql 

`hive -e "select * from test.user1"`      hive -f `use test; select * from user1`

```sql
create table if not exists db_hive_demo.dept(
						deptno int, 
						dname string, 
						loc string)
					row format delimited fields terminated by '\t';
create table if not exists db_hive_demo.emp(
					empno int, 
					ename string, 
					job string, 
					mgr int, 
					hiredate string, 
					sal double, 
					comm double, 
					deptno int)
					row format delimited fields terminated by '\t';
load data local inpath '/dept.txt' into table db_hive_demo.dept;
load data local inpath '/emp.txt' into table db_hive_demo.emp;
					
```



#### Hive 历史命令存放地

cat ~/.hivehistory

主要用于排查逻辑错误或者查看常用命令

#### Hive 临时生效设置

固定语法: set 属性名 = 属性值

set hive.cli.print.header=false

#### HIve内部表和外部表

create table custom_table(id int, name string)

默认是内部表, inner  create inner table, 删除表数据时, 连同数据源以及元数据信息同时删除

显示指定: external, 只会删除元数据信息

相同之处: 如果导入数据时, 操作于hdfs 上 , 会将数据进行迁移, 并在metastore 留下记录, 而不是copy 数据, 也就是**原来数据被删除**

#### HIve 分区表

```sql
create database if not exists db_web_data ;
				create table if not exists db_web_data.track_log(
				id              string,
				url            string,
				)
			partitioned by (date string,hour string) -- 分区表的分区字段以逗号分隔
			row format delimited fields terminated by '\t';
	导入数据到分区表：
oad data local inpath '/home/admin/Desktop/2015082818' into table db_web_data.track_log partition(date='20150828', hour='18');
```

查询

`select url from track_log where date='20150828'` 查询整天的数据

`select url from track_log where date='20150828 and hour = '18'`查询18时的数据

`select url from track_log where date = '20150828' and hour = '18'limit 1`只查询一条

### hive 创建表方式

1. create  

   ```sql
   create table if not exists db_web_data.track_log(字段)
   partitioned by(data string, hour string)
   row format delimited fields terminated by '\t'
   ```

2. 某张表某些字段 `create table back_track_log as select * from db_web_data.track_log`

3. 复制表结构  `create table like_track_log like db_web_data.track_log`

#### Hive 表导入数据方式

1. 本地导入  `load data local inpath 'local_path/file' into table 表名`
2. HDFS 导入  `load data inpath 'hdfs_path/file'into table 表名 `
3. 覆盖导入  `load data local inpath 'path/file' overwrite into table 表名`    `load data inpath 'path/file' overwrite into table 表名`
4. 查询导入 `create table track_log_bak as select * from db_web_data.track_log`
5. insert 导入   追加` insert into table 表名 select * from track_log`     覆盖`insert overwrite table 表名 select * from track_log`  ( 默认追加)

Hive 表导出数据方式

1. 本地导出 `insert overwrite local diretory "/home/admin/Desktop/1/2" row format delimited fields terminated by '\t' select * from db_hive_demo.emp` (可以选择select )
2. hdfs 导出 `insert overwrite diretory "path/"select * from staff`
3. bash shell 覆盖追加导出  `hive -e "select * from staff;" > /home/z/backup.log`
4. sqoop

### HQL 练习

#### 1, 每个部门的最高薪资

```sql
select deptno, MAX(sal)
from emp
group by deptno;
```

#### 2, 查询显示员工姓名, 员工编号, 部门名称

```sql
select e.ename, e.empno, d.dname
from emp e join dept d on e.deptno = d.deptno;
```

#### 3, 按照部门进行薪资的排位

```sql
select empno, ename, sal, deptno, row_number() over(partition by deptno order by sal desc) rank from emp;
```

#### 4, 按照部门进行薪资排名, 显示前两名

```sql
SELECT temp.empno,temp.ename,temp.sal,temp.deptno 
from (select empno, ename, sal, deptno, row_number() over(partition by deptno order by sal desc) rank from emp) temp 
where temp.rank <=2;
```

#### 5, 统计某个网站某天的所有PV数据

```sql
select temp.date, count(temp.url) pv
from ( select substring(trackTime, 0, 10) date, url, 
      from
      db_web_data.track_log
      where length(url) > 0

) temp
group by temp.date;
```

#### 6, 统计某个网站某天的所有UV数据

```sql
select 
temp.date, count(temp.url) pv, count(distinct temp.guid) uv
from (
	select substring(trackTime, 0 , 10) date, url, guid 
    from  db_web_data.track_log
    where length(url) > 0
) temp
group by temp.date;
```

7, 按照省份和日期统计pv和uv

```sql
select data ,provinceId, count(url) pv, count(distinct guid) uv from track_log group by date, provinceId;
```



#### explain

在语句前面加上explain

expressions: sal (type: double), deptno (type: int) 中间表字段

#### case 案例

```sql
select ename, 
case when comm is null then 0 + sal else comm + sal end 
from emp;
```

### HiveServer2 显示格式化好的表

配置 hive-site.xml

配置：hive-site.xml
​	hive.server2.thrift.port --> 10000
​	hive.server2.thrift.bind.host --> hadoop-senior01.itguigu.com
​	hive.server2.long.polling.timeout -- > 5000（去掉L）     

hive.server2.enable.doAs   --> false        允许在hiveserver2 的情况下允许调用mapreduce 任务                                                                      	
检查端口：
​	$ sudo netstat -antp | grep 10000
启动服务：
​	$ bin/hive --service hiveserver2
连接服务：
​	$ bin/beeline
​	beeline> !connect jdbc:hive2://hadoop-senior01.itguigu.com:10000 

select 出来的结果和mysql 相同, 有框包围

#### 自己定义hive 函数

```java
public class ToLowerCase extends UDF{
    public Text evaluate(Text str){
        if(str == null) return null;
        if(str.toString().length() <=0) return null;
        return new Text(Str.toString().toLowerCase());
    }
    public static void main(String[] args){
        System.out.println(new ToLowerCase().evaluate(new Text(arge[0])));
    }
}
```

hive-jdbc  hive-exec

`add jar /home/admin/Desktop/zlowercase.jar`

`create temporary function zlowercase as 'com.z.demo.hive.ToLowerCase'`

### 业务案例

需求：执行周期性任务，每天的晚上6点，执行自动化脚本，加载昨天的日志文件到HDFS，同时分析网站的多维数据（PV,UV按照省份和小时数进行分类查询）最后将查询的结果，存储在一张临时表中（表字段：date，hour，provinceId，pv，uv）存储在HIVE中，并且将该临时表中的所有数据，存储到MySQL中，以供第二天后台开发人员的调用，展示。

1. 定时加载本地数据到hdfs, auto.sh, crontab
2. 清洗数据, 打包jar , 定时执行  `/user/hive/warehouse/db_web_data.db/track_log/date=20150828/hour=18/part-000001`   `/user/hive/warehouse/db_web_data.db/track_log/date=20150828/hour=19part-000001`
3. 建表track_log `alter table track_log add partition(date='20150828',hour='18') location "/user/hive/warehouse/db_web_data.db/track_log/date=20150828/hour=18"`                         `alter table track_log add partition(date='20150828',hour='18') location
    "/user/hive/warehouse/db_web_data.db/track_log/date=20150828/hour=19"`
4. 开始分析想要的数据, 将结果存储在hive 临时表中       创建临时表: ` create table if not exists temp_track_log(date string, hour string, provinceId string, pv string, uv string) row format delimited fields terminated by '\t'`              向临时表中插入数据:  `insert overwrite table temp_track_log select date, hour, provinceId, coount(url) pv, count(distinct guid) uv from track_log where date = '20150828' group by date, hour, provinceId`       
5. 导入mysql  (sqoop)

### Hive 中几种排序

#### order by

 全局排序, 就一个reduce

#### sort by 

相当于对每一个reduce 内部数据排序,不是全局排序

#### distrubute by

类似于MRpartition, 进行分区, 一般要结合sort by 使用

#### cluster by 

当distrubute和sort 字段相同时, 就是cluster by

`insert overwrite local directory "/home/admin/result/order" row format delimited fields terminated by "\t"      select * from emp order by empno`    这个导出的文件是一个

* 对每个部门里面 员工编号排序   `insert overwrite local directory "/home/admin/result/sort" row format delimited fields terminated by "\t" select * from emp distrubute by deptno sort by empno`    一个reduce 部门分不开, 三个reduce 会把部门分开, 一般有几个部门就有几个reduce

### Hive 正则表达式

```sql
create table IF NOT EXISTS db_web_data.baidu_log (
remote_addr string,
remote_user string,
time_local string,
request string,
status string,
body_bytes_sent string,
request_body string,
http_referer string,
http_user_agent string,
http_x_forwarded_for string,
host string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
"input.regex" = "(\"[^ ]*\") (\"-|[^ ]*\") (\"[^\]]*\") (\"[^\"]*\") (\"[0-9]*\") (\"[0-9]*\") (-|[^ ]*) (\"[^ ]*\") (\"[^\"]*\") (-|[^ ]*) (\"[^ ]*\")"
)
STORED AS TEXTFILE;
```

### UDP 时间转换

1. #### 写程序

```java
public class DataTransformUDF extends UDF{
    private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMyy:HH:mm:ss", Locale.ENGLISH);
    	private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	public Text evaluate(Text str){
            Text output = new Text();
            if(str == null) return null;
            if(StringUtils.isBlank(str.toString())) return null;
            Date parseDate;
            try{
                parseDate = inputFormat.parse(str.toString().trim());
                String outputDate = outputFormat.format(parseDate);
                output.set(outputDate);
            }catch(ParseException e){
                e.printStackTrace();
            }
            return output;
    	}
    	public static void main(String[] args){
            System.out.println(new DataTransformUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
    	}
}
```

#### 2. 添加打包后的jar

`add jar /home/admin/Desktop/dataformat.jar`

#### 3 添加临时函数

'create temporary function dateformat as 'com.z.demo.udf.DataTransformUDF'

#### 4测试

java.text.ParseException: Unparseable date: ""31/Aug/2015:00:04:53 +0800""
​        		at java.text.DateFormat.parse(DateFormat.java:366) 

#### 5修复  定义一个去除双引号的

```java
public class RemoveQuotesUDF extends UDF {

	public Text evaluate(Text str){
		if(null == str){
			return null;
		}
		
		// validate 
		if(StringUtils.isBlank(str.toString())){
			return null ;
		}
		
		// replaceAll
		return new Text(str.toString().replaceAll("\"", ""));
	}
	
	public static void main(String[] args) {
		System.out.println(new RemoveQuotesUDF().evaluate(new Text("\"GET /course/view.php?id=27 HTTP/1.1\"")));
//		System.out.println(new RemoveQuotesUDF().evaluate(new Text(args[0])));
	}
}

```

#### 6 再次测试  

`select dateformat(remove_q(time_local)) date from baidu_log limit 1`

date     	2015-08-31 00:04:37
​						Time taken: 0.196 seconds, Fetched: 1 row(s)