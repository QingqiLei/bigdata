### zookeeper

### 理论

#### 1 概论

1. zookeeper 是开源的, 为分布式应用提供协调服务的apache
2. 从设计模式角度来理解: 是一个基于观察者模式设计的分布式服务管理框架, 他负责存储和管理大家都关心的数据, 然后接受观察者注册, 一旦这些数据的状态发生变化, zookeeper 就将负责通知已经在zookeeper 上注册的那些观察者做出相应的反应, 从而实现集群中类似master/slave 管理模式
3. zookeeper = 文件系统 + 通知机制

#### 2 应用场景 

分布式消息同步和协调机制, 服务器节点动态上下线, 统一配置管理, 负载均衡, 集群管理

#### 3 典型应用场景

1. 数据发布与订阅

* 应用启动时主动到zookeeper 上获取配置信息, 并注册watcher 监听
* 配置管理员变更zookeeper 配置节点的内容
* zookeeper 推送变更到应用, 触发watcher 回调函数
* 应用根据逻辑, 主动获取新的配置信息, 更改自身逻辑

2. 软负载均衡

* register 负责域名的注册, 服务启动后将域名信息通过register 注册到zookeeper 相对应的域名服务下
* dispatcher 负责域名的解析, 可以实现软负载均衡
* scanner 通过定时监测服务状态, 动态更新节点地址信息
* monitor 负责手机服务信息与状态监控
* controller 提供后台console, 提供配置管理功能

3. 集群管理

* 有多少机器在工作
* 每台机器的运行状态收集
* 对集群中设备进行上下线操作
* 分布式任务的状态汇报

### 实战

#### 本地安装

加压, 更名 zoo_sample.cfg --> zoo.cfg

修改dataDir

#### 参数

读是follower 负责, leader 负责写

timeTime: 通信心跳数 2000 是2s

initLimit: LF 初始通信时限 leader follow    10 个心跳

synclimit: LF 同步通信时限   5个心跳

dataDir 数据文件目录 + 数据持久化路径 存放数据的路径

clientPort: 客户端连接端口  默认2181

### 理论二

1. #### 数据结构

* 类似unix文件系统, 整体可以看做一棵树
* 每一个被称作  znode, 每个znode 默认能存储1MB的数据, 每个znode 可以通过路径唯一标识



### 选举机制

1. 半数机制: 集群中半数以上机器存活, 集群可用, 所有zookeeper 适合装在奇数台机器上
2. 选举通过内部选举机制临时产生

选举过程

1. 服务器1 启动, 此时它发出的信息没有回应, 所以选举状态是looking 状态
2. 服务器2启动, 和服务器1通信, 由于都没有leader 所以id 为2 获胜,但是没有半数服务器同意选举, 所以还是looking 状态
3. 最后1 2 3 都把票投给了3 , 4 投给了4, 5投给了5, 所以3 是leader

#### 特点

1. 一个领导者(leader, ) 多个跟随者
2. leader 用于进行投票的发起和决议, 更新系统状态
3. follower 用于接受客户请求并向客户端返回结果, 在选举leader 过程中参与投票
4. 只要有**半数以上**的节点存活, zookeeper 集群就能正常服务 (所以奇数好)
5. 全局数据一致, 每个server都保存一份相同的副本, client 无论连接到哪个server, 数据都是一致的
6. 更新请求顺序进行, 来自同一个client 的更新请求按其发送顺序以此执行
7. 数据更新原子性, 一次数据更新要么成功, 要么失败
8. 实时性, 在一定时间范围内, client 能读到最新的数据

#### 节点类型

znode 有两种: 短暂和持久

短暂: 客户端和客户端断开连接后, 创建节点自己删除

持久: 客户端和服务器断开连接后, 创建的节点不能删除

四种形式:

1. 持久化目录节点
2. 持久化顺序编号目录节点
3. 临时目录节点(记录服务器信息)
4. 临时顺序编号目录节点

### 实战二

#### 1 集群

q. server.A=B:C:D

hadoop102:2888:3888

A: 数字 表示这是第几号服务器

B: 服务器ip

C: 服务器与leader 交换信息

D: 万一leader 挂了, 需要一个端口选举

集群模式下配置一个文件myid, 这个文件在dataDir 目录下, 这个文件里面有一个数据就是A的值, Zookeeper启动时就读取此文件, 拿到里面的数据与zoo.cfg里面的配置信息比较从而判断到底是哪个server

#### 启动

/bin/zkServer.sh start

#### 显示节点状态

/bin/zkServer.sh status

### 客户端命令

#### 1 启动客户端

随便在任何一个节点启动客户端都可以

`bin/zkCli.sh`

#### 2 显示所有操作命令

`help`

#### 3 查看当前znode 中所包含的内容

ls /       

#### 4 查看当前节点数据并能看到更新次数等数据

ls2 /

#### 5 创建普通节点

节点上必须有数据  ""

`create /app1 "hello app1"`

#### 6 获得节点的值

get /app1

#### 7 创建短暂节点

create -e /app-emphemeral 8888

退出  quit

进入客户端  ./zkCli.sh

#### 8 创建带序号的节点

create /app2 "app2"

create -s /app2/aa 888

create -s /app2/bb 888

**如果原节点下有一个节点, 则再排序时从1开始, 以此类推** (会加上编号)

#### 9 修改节点数据值

set /app1 999

set /app1 "jia xu hao "

#### 10 节点的值变化监听

在zk1主机上注册坚挺/app1 节点数据变化   get /app1 watch

在 zk2上修改  set /app1 777

观察104 主机收到的数据变化监听

**只生效一次**

#### 11节点的子节点变化监听(路径变化)

在zk1主机上注册坚挺/app1 节点的子节点变化   ls /app1 watch

create /app1/bb 666

观察zk1主机收到子节点变化的监听    get /app1/bb watch 

在zk2 上修改  set /app1/bb 999

**只生效一次**

****

#### 12 删除节点

delete /app1/bb

#### 13递归删除节点

rmr /app2

#### 14查看节点状态

stat /app1     和get /app1 类似

* czxid  创建这个事务的zxid
* ctime -znode 被创建的毫秒数
* mzxid 最后更新的zxid
* mtime 最后修改的毫秒数
* pzxid 最后更新的子节点
* cversion 子节点变化号
* dataversion 数据变化号
* aclversion 访问控制列表的变化号
* ephemeralowner 如果是临时节点  则是znode 拥有者的sessionid, 如果不是临时节点则是0
* datalength 数据长度
* numchildren 子节点数量



### 优化

zookeeper.session.timeout

hbase-site.xml

该值会直接关系到master发现服务器宕机的最大周期，默认值为30秒，如果该值过小，会在HBase在写入大量数据发生而GC时，导致RegionServer短暂的不可用，从而没有向ZK发送心跳包，最终导致认为从节点shutdown。一般20台左右的集群需要配置5台zookeeper。