### 

#### 特点

1. hbase 存储的所有数据本质存储在datanode 上面, 间接存储在regionserver上面,(regionserver 数据直接存储在datanode 上), 视觉上可以看到 Hmaster 和Hregionserver, 元数据存储在zookeeper上面,
2. 关系型数据库, 数据以表的形式存在, 而hbase 存在hdfs 上面
3. regionserver 可以包含多个hregion, 每个regionserver 有一个hlog, 多个hfiles,
4. hdfs 发生断电错误,可能丢失数据, 没有事务处理, 而mysql 不允许发生, Hbase 优化了
5. 数据传来regionserver 先于内存写入hlog, 写入hlog 成功下一步放入内存,如果成功写入内存就成功了, 如果内存中数据超过64MB(或者达到堆内存的40%), 就会把内存中数据吸入region 上面.
6. 如果内存往region 写的时候断电了, 那么来电后region 会重新读内存, 把旧的脏数据删除.
7. Hbase 一般放在关系型数据库后面,定期从关系型数据库同步数据到Hbase 中, 不过频率很低 
8. hbase 使用 wal   (write ahead logs) 存储日志
9. hmaster 如果启动成功, 会向zookeeper 里面拿数据的元数据(存在region中数据的元数据),
10. 客户端访问hmaster 请求数据, hmaster 返回数据所处region 位置信息放回给客户端, 客户端操作regionserver(和namenode 不同),即使hmaster 挂了, 仍然可以频繁读写regionserver
11. 关系型数据库使用主键, hbase 使用rowkey
12. `put "rowkey""cf:cn" (时间戳)数据`
13. bytheway, hlog 保存的是数据和数据对应的操作(预写入操作), hlog 保存在hdfs, hlog 写入成功后, 会放在内存中, 然后放入region 中,写入region 成功后, hlog 会被删除, 只保存region 中的数据



再来一遍:

1. 客户端通过zookeeper找到hmaster 地址,再找到hregionserver
2. hdfs 适合一次写入, 多次读取
3. region 里的数据由rowkey 唯一标示, 有一个row key 的范围, 放row key 范围越来越大, 会被分开
4. 默认通过hash 值排序rowkey,默认rowkey 范围0 到无穷大

一个regionserver 可以包含多个region, 每个regionserver 维护一个hlog, 多个hfiles,  memstore, regionserver 运行在datanode上, 数量可以和datanode 一致

可以有多个regionserver, 一个regionserver 包含两个东西 一个hlog 和多个region,  region可以有多个,  region 中有多个store, store 中有一个memstore 和多个storefile, 每个storefile 有一个hfile. 

当region 很多的时候, 我们切割的是region, 不是hfile, 一个region 里有多个store, 一个store 有一个memstore, 多个hfile, 切割region 就是逻辑上的划分, 也就是切割元数据信息, 也就是指向做了变更

####  为什么切分

1. 方便负载均衡
2. 查询快
3. 防止数据倾斜( 一台服务器很忙, 一台服务器很闲,  查询的问题, 数据是不是热点)

#### 与RDBMS 对比

向上拓展        横向拓展

sql查询            使用api 和mapreduce 来访问hbase 表数据

面向行             面向列

依赖服务器配置    依赖机器数量

支持acid          不支持acid

### 特征

#### 1 自动故障处理, 负载均衡

hbase 运行在hdfs上, 所以hbase 中数据是多副本的.

#### 2 自动分区

当region 增长到一个阈值, 可以将region 切分

#### 3 lsm tree

随时大数据访问, 定期将hdfs 上小文件合成为大文件,LSM, 设计思想:**将对数据的修改增量保持在内存中，达到指定的大小限制后将这些修改操作批量写入磁盘**，**LSM树原理把一棵大树拆分成N棵小树，它首先写入内存中，随着小树越来越大，内存中的小树会flush到磁盘中，磁盘中的树定期可以做merge操作，合并成一棵大树，以优化读性能。**

hbase在实现中，是把整个内存在一定阈值后，flush到disk中，形成一个file，这个file的存储也就是一个小的B+树，因为hbase一般是部署在hdfs上，hdfs不支持对文件的update操作，所以hbase这么整体内存flush，而不是和磁盘中的小树merge update，这个设计也就能讲通了。内存flush到磁盘上的小树，定期也会合并成一个大树。整体上hbase就是用了lsm tree的思路。

#### 4 java api

#### 5 横向扩展

#### 6 列存储

### 缺点

1. 对join , 多表合并数据查询性能不好
2. 更新
3. 没有索引

### 安装

#### 1 环境

* 必须有hdfs, yarn zookeeper, 这些可以不在一个节点上, 

* hbase 也可以和hdfs, yarn, zookeeper 不在一个节点上

* hbase 支持的hadoop 版本有限, 进入lib 目录查看所带hadoop 的jar 包版本, 对应的hadoop 版本
* 需要ssh

#### 2 解压

`tar -xf`

#### 3 修改配置文件

1. hbase-env.sh 

   ```shell
   export JAVA_HOME=
   export HBASE_MANAGES_ZK=false
   ```

2. hbase-site.xml 

   ```xml
   <configuration>
       <property>
           <name>hbase.rootdir</name>
           <value>hdfs://hadoop-1:9000/hbase</value>
       </property>
       <property>
           <name>hbase.cluster.distributed</name>
           <value>true</value>
       </property>
       <property>
           <name>hbase.master.port</name>
           <value>60000</value>
       </property>
       <property>
           <name>hbase.zookeeper.quorum</name>
           <value>zk1:2181,zk2:2181,zk3:2181</value>
       </property>
       <property>
           <name>hbase.zookeeper.property.dataDir</name>
           <value>/usr/local/zookeeper-3.4.13/data/zkData</value>
       </property>
   </configuration>
   ```

3. regionservers 

   ```jshell
   hbase-1
   hbase-2
   hbase-3
   ```

#### 4 jar

更换jar 包, 如果hadoop 版本相同, zookeeper 版本不同, 只更换zookeeper就行了  `zookeeper-3.4.10.jar`

#### 5 hadoop 配置文件

将core-site.xml 和hdfs-site.xml 软链接到hbase conf中

#### 6 HMaster 高可用

1. 确保hbase 集群已经停止
2. 在conf 目录下创建  backup-masters
3. 在 backup-masters 写入备用的主机名
4. 将backup-masters 同步到其他hbase 节点
5. 启动hbase 集群

#### 7 启动集群

**保证 hdfs yarn zookeeper 都已经正常启动**

`bin/hbase-daemon.sh start master`

`bin/hbase-daemon.sh start regionserver`

或者

`bin/start-hbase.sh`

`bin/stop-hbase.sh`

#### 8 web

hbase-1:16010

hbase-1:16030

### 常用操作

#### 1 进入 hbase 客户端命令操作界面

`bin/hbase shell`

#### 2 帮助 

help

#### 3 查看表

list

#### 4 创建表

create 'student','info'

#### 5 put

`put 'student','1001','info:name', 'Thomas'`

`put 'student', '1002', 'info:sex','male'`

`put 'student','1003','info:age','18'`

#### 6 扫描查看数据

`scan 'student'`

`scan 'student', {STARTROW=>'1001', STARTROW=>'1002'}`

#### 7 查看表结构

`describe 'student'`

#### 8 更新指定字段数据( 还是put)

`put 'student','1001', 'info:name', 'Nick'`

`put 'student','1001', 'info:age','1000'`

#### 9 查看指定行数据

`get 'student','1001'`

`get 'student', '1001', 'info:name'`

#### 10 删除数据

deleteall 'student', '1001'

delete 'student', '1001', 'info:sex'

#### 11 清空表数据

'truncate 'student'

#### 12 删除表

`disable 'student'`

`drop 'student'`

#### 13 统计表的行数

`count 'student'`

#### hbase 读写流程

 zookeeper 保存这regionserver 的元数据信息, regionserver 保存region的元数据信息

#### 读

1. client 访问zookeeper, 获取regionserver 的元数据信息
2. client访问regionserver , 请求region 的元数据信息
3. client 访问具体存放于region 中的数据

#### 写

1. 访问zookeeper, 获取regionserver 的信息
2. 访问regionserver, 把操作写入hlog(不在hdfs上面) 中,  然后往MeMs 写,如果达到64MB, 或者达到40%, 就往datanode 上面写
3. hdfs 上面storefile 越来越多, 会触发compact 合并操作, 把过多的storefile 合并为一个大的storefile, 
4. 当storefile 越来越大, region 也越来越大, 达到阈值后, 出发split 操作, 将region一分为二

#### HMASTER 的作用

1. 负责regionserver 的负载均衡(如果hmaster 挂了, 那么regionserver 不会随机应变)
2. 调度regionserver 去执行分割region 的任务

#### regionserver 的作用

1. 切片 region
2. 数据读取
3. 数据写入
4. 文件合并
5. WAL 机制
6. 内存溢写

### hive 和hbase

hive 是数据仓库  hbase 是数据库

### hive

#### 1 数据仓库

本质就是将hdfs 中已经存储的文件在mysql 中做了一个双射关系, 方便使用hsl 查询

#### 2 用于数据分析, 清洗

适合离线的, 延迟很高

#### 3 基于hdfs, mapreduce

将sql 语句转成mapreduce 任务

### hbase

#### 1 数据库 

面向列存储的非关系型数据库

#### 2 用于结构化和非结构化数据

不适合关联关联查询

#### 3 基于hdfs

数据持久化存储的体现形式是hfile, 存放在datanode, 被regionserver 以region的形式管理

#### 4 延迟低, 接入在线业务使用



### hbase 和hive 交互操作

#### 1 环境

因为操作hive 同时对hbase 也有影响, 所以把hive要有hbase 的jar,所以软连接hbase 的jar到hive 中

hbase-common      hbase-server           hbase-client   hbase-protocol        hbase-it  htrace-core    hbase-hadoop2-compat        high-scala-lib 

同时修改hive-site.xml 中zookeeper属性

```xml
<property>
    <name>hive.zookeeper.quorum</name>
    <value>zk-1,zk-2,zk-3</value>
</property>
```

案例1 插入hive 表 同时影响hbase

step 1

```sql
CREATE TABLE hive_hbase_emp_table(
empno int,
ename string,
job string,
mgr int,
hiredate string,
sal double,
comm double,
deptno int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:comm,info:deptno")
TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");
```

(完成后, 可以分别进入hive 和hbase 查看, 都生成了对应的表)

step 2 

在hive 中创建临时中间表, 用户load 数据 (不能将数据直接load 进所关联的hive 中)

```sql
CREATE TABLE emp(
empno int,
ename string,
job string,
mgr int,
hiredate string,
sal double,
comm double,
deptno int)
row format delimited fields terminated by '\t';
```

step 3 load

`load data local inpath '/home/admin/Desktop/emp.txt' into table emp;`

step 4 insert 

`insert into table hive_hbase_emp_table select * from emp`

#### 案例2 hbase 已经有表了, 创建hive 表分析hbase 的表

step1

```sql
CREATE EXTERNAL TABLE relevance_hbase_emp(
empno int,
ename string,
job string,
mgr int,
hiredate string,
sal double,
comm double,
deptno int)
STORED BY 
'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = 
":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:comm,info:deptno") 
TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");
```

step2 已经可以了

`select * from relevance_hbase_emp;`

### hbase sqoop

#### 1 将rdbms 中数据抽取到hbase 中 

记着要有mysql 授权(可以**远程登录**)

step 1  sqoop-env.sh 配置好

step mysql 创建表

```sql
CREATE DATABASE library;
CREATE TABLE book(
id int(4) PRIMARY KEY NOT NULL AUTO_INCREMENT, 
name VARCHAR(255) NOT NULL, 
price VARCHAR(255) NOT NULL);
```

step3 插入数据

```sql
INSERT INTO book(name, price) VALUES('Lie Sporting', '30');  
INSERT INTO book (name, price) VALUES('Pride & Prejudice', '70');  
INSERT INTO book (name, price) VALUES('Fall of Giants', '50');  
```

step 4 sqoop

```shell
bin/sqoop import \
--connect jdbc:mysql://hadoop-1:3306/db_library \
--username root \
--password 123456 \
--table book \
--columns "id,name,price" \
--column-family "info" \
--hbase-create-table \
--hbase-row-key "id" \
--hbase-table "hbase_book" \
--num-mappers 1  \
--split-by id
```

step 5 在hbase 中scan 这张表  `scan 'hbase_book' `

### hbase 数据大小

#### 固定大小

RowKey：6个字节

Value：4个字节

TimeStamp：9个字节

KeyType：1个字节

CF：1个字节

#### 可变大小



### hbase shell

#### status

#### whoami

#### list

#### count 统计指定表的记录数

#### describe 展示表信息

#### exist 检查表存在

#### is_enabled  is_disabled 检查表禁用

#### alter 改变columnfamily

#### 增加列家 `alter 'book', name=>'cf', version=>5`

#### 删除列家 

`alter 'book', 'delete'=>'cf'`  

#### disable

#### drop

#### delete

`delete 'book', 'rowkey', 'cd:c'`

#### truncate

禁用表  删除表 创建表

#### create

#### `create 'table', 'cf'`

#### `create 't1', {NAME=>'F1'},{NAME => 'F2'}, {NAME => 'F3'}`

### hbase 节点管理

#### 1 服役

当启动regionserver 时候, regionserver 向hmaster 注册开始接受数据, 注意将主机名加入  conf/regionserver 中

#### 2 退役

step 1 停止负载均衡

`hbase > balance_switch false`

step 2 在退役节点上停止regionserver

`bin/hbase-daemon.sh stop regionserver`

step 3 regionserver 一旦停止, 会关闭维护的所有region

step4 zookeeper 上regionserver 消失

step 5 master 检测到regionserver 下线

step 6 master 重新分配region

**方法二 简单**

step1 regionserver 卸载 所管理的region

`bin/graceful_stop.sh hbase-3`

step 2 自动平衡数据

step 3 和前面的 2~6 相同 (停止regionserver)

### 预分区

每个region 维护着startrow 和endrowkey , 如果数据符合某个region 维护的rowkey范围, 那么数据交给这个region 维护

#### 手动指定预分区

`create 'staff', 'info', 'partition1', SPLITS=>['1000','2000','3000','4000']`

16 进制数组的row key `create 'staff2', 'info', 'partition2', {NUMREGIONS=>15, SPLITALGO=>'HexStringSplit'}` 

### rowkey 设计

#### 随机数 hash md5



#### 字符串反转

20170524000001  ->10000042507102

20170524000002转成20000042507102

这样也可以在一定程度上散列逐步put进来的数据

#### 字符串拼接

20170524000001_a12e

### 优化

#### 在HDFS的文件中追加内容

dfs.support.append   hdfs-site.xml、hbase-site.xml  开启HDFS追加同步，可以优秀的配合HBase的数据同步和持久化。默认值为true。

### 优化DataNode允许的最大文件打开数

dfs.datanode.max.transfer.threads    hdfs-site.xml

#### 优化延迟高的数据操作的等待时间

dfs.image.transfer.timeout  hdfs-site.xml        建议把该值设置为更大的值（默认60000毫秒）

### 优化数据的写入效率

mapreduce.map.output.compress

mapreduce.map.output.compress.codec

mapred-site.xml           开启这两个数据可以大大提高文件的写入效率，减少写入时间。第一个属性值修改为true，第二个属性值修改为：org.apache.hadoop.io.compress.GzipCodec

### 优化DataNode存储

dfs.datanode.failed.volumes.tolerated             hdfs-site.xml

默认为0，意思是当DataNode中有一个磁盘出现故障，则会认为该DataNode shutdown了。如果修改为1，则一个磁盘出现故障时，数据会被复制到其他正常的DataNode上，当前的DataNode继续工作。

#### 设置RPC监听数量

hbase.regionserver.handler.count             hbase-site.xml              默认值为30，用于指定RPC监听的数量，可以根据客户端的请求数进行调整，读写请求较多时，增加此值。 

#### 优化HStore文件大小

hbase.hregion.max.filesize            hbase-site.xml

默认值10737418240（10GB），如果需要运行HBase的MR任务，可以减小此值，因为一个region对应一个map任务，如果单个region过大，会导致map任务执行时间过长。该值的意思就是，如果HFile的大小达到这个数值，则这个region会被切分为两个Hfile。

#### 优化hbase客户端缓存

hbase.client.write.buffer             hbase-site.xml            用于指定HBase客户端缓存，增大该值可以减少RPC调用次数，但是会消耗更多内存，反之则反之。一般我们需要设定一定的缓存大小，以达到减少RPC次数的目的。

#### 指定scan.next扫描HBase所获取的行数

hbase.client.scanner.caching

hbase-site.xml          用于指定scan.next方法获取的默认行数，值越大，消耗内存越大。

### jvm hbase-env.sh

#### 并行gc

-XX:+UseParallelGC

#### 同时处理垃圾回收的线程数

-XX:ParallelGCThreads=cpu_core – 1

### 禁用手动GC

-XX:DisableExplicitGC





