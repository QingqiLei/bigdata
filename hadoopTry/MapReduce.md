### mapreduce 程序运行流程分析

1. 待处理文本 /user/atguigu/input   ss.txt  bb.txt
2. 客户端submit() 前, 获取待处理数据的信息, 然后根据参数配置, 形成一个任务分配的规划
3. 提交切片信息, 把信息都提交到yarn 上 (集群上是yarn) Mr appmaster, NodeManager
4. 计算出maptask 数量: 
5. 读取数据 inputformat 一行一行读数据, 以kv 对形式存储
6. 逻辑计算, 把kv 对给wordcount Mapper
7. 收集kv 到缓存
8. 按照k 分区排序(hash 排序)后写入磁盘
9. 所有的maptask 任务完成后, 启动相应数量的reducetask, 并告知reducetask 处理数据范围(一个reduce 只关心自己负责的)
10. 输出结果到文件

#### 并行度

并行度问题就是maptask 开几个

比如有一个ss.avi, 存储在三个datanode 上面,datanode1 0~128, datanode2 128 到256, datanode3 256 ~300

1. 一个job的map阶段并行度由客户端在提交job时决定
2. 每一个数据切片分配一个maptask 并行实例处理
3. 默认情况下 切片大小 =hdfs blocksize
4. 切片不考虑数据集整体, 而是逐个针对每个文件单独切片

#### 切片

默认情况 切片大小 = blocksize

开始切, 每次切片时，都要判断切完剩下的部分是否大于块的1.1倍，不大于1.1倍就划分一块切片

conbineFileInputFormat 切片

1. 默认情况下TextInputformat对任务的切片机制是按文件规划切片，不管文件多小，都会是一个单独的切片，都会交给一个maptask，这样如果有大量小文件，就会产生大量的maptask，处理效率极其低下。

方法:

1. 最好的办法，在数据处理系统的最前端（预处理/采集），将小文件先合并成大文件，再上传到HDFS做后续分析。

2. 补救措施：如果已经是大量小文件在HDFS中了，可以使用另一种InputFormat来做切片（CombineTextInputFormat），它的切片逻辑跟TextFileInputFormat不同：它可以将多个小文件从逻辑上规划到一个切片中，这样，多个小文件就可以交给一个maptask。

3. 优先满足最小切片大小，不超过最大切片大小

   ​		CombineTextInputFormat.*setMaxInputSplitSize*(job, 4194304);// 4m

   ​		CombineTextInputFormat.*setMinInputSplitSize*(job, 2097152);// 2m

   ​	举例：0.5m+1m+0.3m+5m=2m + 4.8m=2m + 4m + 0.8m

#### shuffle

Mapreduce确保每个reducer的输入都是按键排序的。系统执行排序的过程（即将map输出作为输入传给reducer）称为shuffle。

将maptask 输出的处理结果数据, 分发给reducetask, 并在分发的过程中, 对数据按key 进行了分区和排序

1. 待处理文本
2. 客户端submit()前, 获取待处理数据的信息, 然后根据参数配置,形成一个任务分配的规划
3. 提交切片信息
4. MR appmaster 计算出maptask 数量
5. 默认 TextInputFormat (可以自己写)
6. 逻辑计算 Mapper map(), COntext.write(k, v)
7. 环形缓冲区 默认100M
8. 分区,排序  hashPartitioner, Key.compareTo, Combiner
9. 溢出文件(分区且内有序) (将缓冲区一部分数据读出, 放到 分析排序, ) 保存到磁盘上
10. merge 归并排序(放到磁盘上)

hadoop 是基于磁盘的, 很慢

#### 其中shuffle 过程详解

1. maptask 收集我们的map() 方法输出的kv 对, 放到内存缓冲区中
2. 从内存缓冲区不断溢出本地磁盘文件, 可能会溢出多个文件
3. 多个溢出文件会被合并成更大的溢出文件
4. 在溢出的过程中, 及合并过程中, 都需要调用partitioner 进行分组和针对key 进行排序
5. reductask 根据自己的分区号, 去各个maptask 机器上取相应的结果分区数据
6. reducetask 会取到同一个分区的来自不同maptask 的结果文件, reducetask 会将这些文件再进行合并(归并排序)
7. 合并成大文件后, shuffle的过程也就结束了, 后面进入reducetask 的逻辑运算过程(从文件中取出一个一个的键值对group, 调用用户自定义的reduce() 方法)

注意:

shuffle 中的缓冲区大小会影响到 mapreduce 程序的执行速率, 原则上说, 缓冲区越大, 磁盘io 次数越少, 执行速度越

#### 分区 partition 

将统计结果按照条件输出到不同的文件中, 比如: 将统计结果按照手机归属地不同省份输出到不同文件

#### combiner合并

1）combiner是MR程序中Mapper和Reducer之外的一种组件

2）combiner组件的父类就是Reducer

3）combiner和reducer的区别在于运行的位置：

Combiner是在每一个maptask所在的节点运行

Reducer是接收全局所有Mapper的输出结果；

4）combiner的意义就是对每一个maptask的输出进行局部汇总，以减小网络传输量

6）combiner能够应用的前提是不能影响最终的业务逻辑，而且，combiner的输出kv应该跟reducer的输入kv类型要对应起来

### yarn

1. yarn 不清楚用户提交的程序运行机制
2. yarn 只提供运算资源的调度(用户程序向yarn 申请资源, yarn 就负责分配资源)
3. yarn 中的主管角色叫 resourcemanager
4. yarn 中具体提供运算资源的角色叫nodemanager
5. 这样, yarn 就和运行的用户程序完全解耦, 就意味这yarn 上可以运行各个类型的分布式运算程序(mapreduce , storm spark)
6. yarn 就成为了一个通用的资源调度平台, 

工作机制

0. Mr程序提交客户端所在节点 job.submit(), yarnrunner
1. 向resourcemanager 申请一个applicaiotn
2. 返回application 资源提交路径  hdfs:// .../.sraging 以及application_id
3. 提交job 运行所需资源
4. 资源提交完毕, 申请运行mrAppMaster
5. ResourceManager 将用户的请求初始化成一个task
6. nodemanager 从FIFO 调度队列中领取 任务
7. nodemanager 创建容器 cpu + ram MRAppmaster
8. 下载jpb 资源到本地
9. 向 ResourceManager 申请运行maptask 容器
10. 领取到任务, 创建容器 container : cpu + ram+ jar 
11. 发送程序启动脚本, map
12. 向ResourceManager 申请两个 容器, 运行reduce task 程序
13. reduce 向map 获取相应分区的数据

### 优化

#### job 任务服务线程数调整

mapreduce.jobtracker.handler.count 默认10,  50~100

#### http 服务器线程数

mapreduce.tasktracker.http.threads, 默认40, 大集群可以是80~100

#### 文件排序合并优化

mapreduce.task.io.sort.factor 默认值为10，如果调高该参数，可以明显减少磁盘IO，即减少文件读取的次数。

#### 设置任务并发

该属性可以设置任务是否可以并发执行，如果任务多而小，该属性设置为true可以明显加快任务执行效率，但是对于延迟非常高的任务，建议改为false，这就类似于迅雷下载。

### MR输出数据的压缩

mapreduce.map.output.compress、mapreduce.output.fileoutputformat.compress      对于大集群而言，建议设置Map-Reduce的输出为压缩的数据，而对于小集群，则不需要。

### 优化Mapper和Reducer的个数

mapreduce.tasktracker.map.tasks.maximum

mapreduce.tasktracker.reduce.tasks.maximum

以上两个属性分别为一个单独的Job任务可以同时运行的Map和Reduce的数量。

设置上面两个参数时，需要考虑CPU核数、磁盘和内存容量。假设一个8核的CPU，业务内容非常消耗CPU，那么可以设置map数量为4，如果该业务不是特别消耗CPU类型的，那么可以设置map数量为40，reduce数量为20。这些参数的值修改完成之后，一定要观察是否有较长等待的任务，如果有的话，可以减少数量以加快任务执行，如果设置一个很大的值，会引起大量的上下文切换，以及内存与磁盘之间的数据交换，这里没有标准的配置数值，需要根据业务和硬件配置以及经验来做出选择。













