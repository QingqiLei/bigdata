### 概述

#### 1 是什么

1 开源的消息系统, 由scala 写成

2 由linkedln开发, 是一个分布式消息队列

3 kafka 对消息保存时根据topic 进行归类, 发送消息者是producer, 消息接受者称为consumer, 一个kafka 有多个kafka 实例, 每个实例称为broker

4 无论kafka 集群, producer 和consumer 都依赖zookeeper

#### 2 消息模式

1 点对点  一对一, 消费者主动拉取数据, 消息收到后消息清除

2 发布/订阅模式, 一对多, 数据产生后, 推送给所有订阅者

#### 为什么需要消息队列

1 解耦

2 冗余

3 扩展性

4 灵活性 峰值处理能力

5 可恢复性

6 顺序保证

7 缓冲

### kafka架构

1 producer : 消息生产者, 向kafka broker 发消息的客户端

2 consumer: 消息消费者, 向kafka broker 取消息的客户端

3 consumer group(cg), 一个topic 可以有多个cg, topic的消息会复制, 到所有的cg,但每个partion只会把消息发送给cg 中的一个consumer, 如果需要实现广播, 只要每个consumer 有一个独立的cg 就可以了, 要实现单播只要所有的consumer在同一个cg, 用cg 还可以将comsumer 进行自由的分组, 而不需要多次发送消息到不同的topic 

4 broker, 一台kafka 服务器就是一个broker, 一个集群由多个broker 组成, 一个broker 可以容纳多个topic

5 partition 为了可拓展性, 一个非常大的topic 可以分布到多个broker 中, 一个topic 一个分为多个partition, 每个partiiton 是一个有序的队列. partition 中每条消息都会被分配一个有序的id, (offset), kafka 只保证一个partition 中的顺序将消息发送给consumer, 不保证一个topic 的整体(多个parittion 之间)的顺序

6 offset, kafka的存储文件按照offset.kafka来命名, 用offset 做名字的好处是方便查找, 如果想找2049的位置, 只要找到2048.kafka 的文件就行了

### 配置文件

```
broker.id=0  # 不能重复
delete.topic enable=true   # 可以删除topic
num.network.threads=3
num.io.thread=8
socket.sent.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/usr/local/kafka_2.11-2.1.1/logs
num.partitions=1  # topic 在当前broker 上的分区数量
num.recovery.threads.per.data.dir=1 
log.retention.hours=168
zookeeper.connect=zk-1:2181,zk-2:2181
```

#### 后台进程

直接在后台 启动

`bin/kafka-server-start.sh config/server.properties &`

### 命令操作

#### 查看topic

`kafka-topics.sh --list --zookeeper zk-1:2181`

#### 创建topic

`kafka-topics.sh --create --zookeeper zk-1:2181 --partitions 3 --replication-factor 3 --topic second`

--replication-factor: 定义副本数, 不能超过kafka 节点的数量

--partitions 定义分区数

#### 删除topic

`kafka-topics.sh  --delete --zookeeper zk-1:2181 --topic first`

#### 发送消息

`kafka-console-producer.sh  --broker-list kafka-3:9092 --topic first`

#### 消费消息

`kafka-console-consumer.sh  --bootstrap-server kafka-3:9092 --from-beginning --topic first`

#### 查看某个topic 的详情

`kafka-topics.sh  --topic first --describe --zookeeper zk-1:2181`\

### 工作流程分析

#### 写入方式

producer 采用推模式将消息发布到broker, 每条消息都被追加到分区中, 属于顺序写磁盘(顺序写磁盘效率比随机写内存要高, 保障kafka 的吞吐率)

消息被发送到一个topic, 其本质就是一个目录, 而topic是由一些partition 组成, 每个partition 中的消息都是有序的, 消息被不断追加到partition log 上, 其中每个消息被赋予一个唯一的offset 值

#### partition好处

* 方便在集群中扩展, 整个集群可以适应任意大小的数据
* 可以提高并发, 因为可以以partition为单位读写.

#### 副本 replication

同一个partition 可以会有多个partition, (对应server.properties 中default.replication.factor = N), 在没有replication的情况下, 一旦broker 宕机, 其上所有patition 的数据不可被消费, 同时producer 也不能再将数据存于其上的partition . 引入replication 之后, 同一个partition 可能会有多个replication, 这时需要在这些replication之间选出一个leader, producer 和consumer 只与这个leader 交互, 其他replication 作为follower 从leader 中复制数据

#### 写入数据流程

1. producer 从zookeeper "/brokers/.../state" 节点找到该partition的leader
2. producer 将消息发送给该leader
3. leader 将消息写入本地log
4. followers 从leader pull 消息, 写入本地log 后向leader 发送ack
5. leader 收到所有ISR 中的replication 的ack, 增加 hw, 并向producer 发送ack

#### 存储方式

把一个topic 分成多个partition, 每一个partition 对应一个文件夹, 文件夹中有partition 的所有消息和索引文件

#### 存储策略

基于时间  `log.retention.hours=168`

基于大小  `log.retention.bytes=1073741824`

kafka 读取特定消息的时间复杂度是O(1) , 与文件大小无关, 所以这里删除过期文件与提高kafka 性能无关

#### zookeeper 元数据

`get  /brokers/topics/first/partitions/0/state`

`{"controller_epoch":7,"leader":2,"version":1,"leader_epoch":0,"isr":[2,0,1]}`

**没有生产者的信息**

#### 两个api

**高级api** 写起来简单, 不需要自己管理offset, 不需要管理分区, 副本, 自动从zookeeper 中的offset 去读取数据, 可以使用group 来区分同一个topic, 不同的程序访问分离开来(不同的group 记录不同的offset, 这样不同的程序读取同一个topic 才不会因为offset 互相影响)

高级api 缺点 : 不能自行控制offset

**低级api**: 自己控制offset, 想从哪里读就从哪里读

#### 消费者组

由一个或者多个消费者组成一个组, 一个分区在同一时间只能由group 中一个消费者读取, 但是多个group 可以同时消费这个partition, 消费者可以通过水平扩展的方式同时读取大量的消息。另外，如果一个消费者失败了，那么其他的group成员会自动负载均衡读取之前失败的消费者读取的分区。

#### 消费方式

一般是pull, , 由consumer 自主控制消费消息的速率

#### 消费者组案例

测试同一个消费者组中的消费者, 同一时刻只能有一个消费者消费

1 修改consumer.properties, group.id=group1

2 分别启动消费者   `kafka-console-consumer.sh  --bootstrap-server kafka-3:9092 --from-beginning --topic first`

`kafka-console-consumer.sh  --bootstrap-server kafka-3:9092 --from-beginning --topic first`

3 生产者  `kafka-console-producer.sh  --broker-list kafka-3:9092 --topic first`

4 查看消费者, 同一时刻, 只有一个消费者接受到消息

### 拦截器

对于producer而言，interceptor使得用户在消息发送前以及producer回调逻辑前有机会对消息做一些定制化需求，比如修改消息等。同时，producer允许用户指定多个interceptor按序作用于同一条消息从而形成一个拦截链(interceptor chain)。Intercetpor的实现接口是org.apache.kafka.clients.producer.ProducerInterceptor，

（1）configure(configs)

获取配置信息和初始化数据时调用。

（2）onSend(ProducerRecord)：

该方法封装进KafkaProducer.send方法中，即它运行在用户主线程中。Producer确保在消息被序列化以计算分区前调用该方法。用户可以在该方法中对消息做任何操作，但最好保证不要修改消息所属的topic和分区，否则会影响目标分区的计算

（3）onAcknowledgement(RecordMetadata, Exception)：

该方法会在消息被应答之前或消息发送失败时调用，并且通常都是在producer回调逻辑触发之前。onAcknowledgement运行在producer的IO线程中，因此不要在该方法中放入很重的逻辑，否则会拖慢producer的消息发送效率

（4）close：

关闭interceptor，主要用于执行一些资源清理工作

如前所述，interceptor可能被运行在多个线程中，因此在具体实现时用户需要自行确保线程安全。另外倘若指定了多个interceptor，则producer将按照指定顺序调用它们，并仅仅是捕获每个interceptor可能抛出的异常记录到错误日志中而非在向上传递。这在使用过程中要特别留意。



### kafka stream

spark 和storm 都有流处理, kafka stream 有什么优势

1. Kafka Stream提供的是一个基于Kafka的流式处理类库。
2. Kafka Stream作为类库，可以非常方便的嵌入应用程序中，它对应用的打包和部署基本没有任何要求。
3. 大部分流式系统中都已部署了Kafka，此时使用Kafka Stream的成本非常低。
4. 使用Storm或Spark Streaming时，需要为框架本身的进程预留资源
5. Kafka本身提供数据持久化
6. Kafka Stream可以在线动态调整并行度。



