[TOC]



### 配置文件

1. core-site.xml, 配置hadoop.tmp.dir 和 fs.defaultFS
2. hdfs-site.xml, 配置 dfs.replication  
3. yarn-site.xml, 配置yarn.nodemanager.aux-services和 yarn.resourcemanager.hostname
4. mapred-site.xml, 配置 mapreduce.framework.name

wget http://download.oracle.com/otn-pub/java/jdk/8u181-b13/96a7b8442fe848ef90c96a2fad6ed6d1/jdk-8u181-linux-x64.tar.gz?AuthParam=1534129356_6b3ac55c6a38ba5a54c912855deb6a22

### 集群配置

docker run -h hadoop-1 -dit -v /configure:/configure hadoop:v3 /bin/bash

#### /etc/hosts

配置id 和主机名

#### ssh 

实现的就是登录另一台机器不需要输入密码

公钥:公开的, 

私钥: 私有的

`/usr/sbin/sshd`

将Ａ的公钥放在Ｂ里, 当A访问B(数据用私钥A加密), 接收到数据后, 去授权key 中查找A的公钥, 并解密数据, B 需要回复, 采用A公钥加密的数据返回给A, A收到数据,用A 的私钥解密数据

1. 进入到我的家目录  (root 用户的家目录在 /root, atguigu 用户家目录在/home/atguigu  配置完ssh)
2. 生成公钥和私钥
3. 将公钥拷贝到要免密登录的目标机器上（拷贝到`~/.ssh/authorized_keys`）(使用 `ssh-copy-id 另一台ip`)

#### 配置文件

core-site.xml

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop-1:9000</value>
    </property>
    <property>
         <name>hadoop.tmp.dir</name>
         <value>/usr/local/hadoop/hadoop_tmp</value>
         <description>A base for other temporary directories.</description>
     </property>
</configuration>
```

hdfs-site.xml

```xml
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
      <name>dfs.namenode.secondary.http-address</name>
      <value>hadoop-2:9868</value>
    </property>
```

mapred-site.xml

```xml
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
```

yarn-site.xml

```xml
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>

<property>
        <name>yarn.resourcemanager.hostname</name>
        <value>hadoop-3</value>
</property>
```

workers

```
hadoop-2
hadoop-3
hadoop-4
hadoop-5
```

#### 启动hdsf

#### 启动yarn :

 如果Namenode 和ResourceManager 如果不是同一台机器, 不能在NameNode上启动yarn, 应该在resourceManager 所在的机器上启动yarn

#### web

hadoop 有两个web 

* namenode   50070 或者9870
* yarn  8088



### hdfs 命令

#### -ls

`hdfs dfs -ls `

`hdfs dfs -ls /`

#### -mkdir

`hdfs dfs -mkdir input`

`hdfs dfs  -mkdir -p /usr/chauncey/test`

#### -moveFromLocal

移动, 不是复制

`hdfs dfs -moveFromLocal /usr/local/output.txt /usr/chauncey/test`

#### -moveToLocal



#### -appendToFile

`hdfs dfs -appendToFile df /user/chauncey/test/read`

#### -cat

`hdfs dfs -cat /user/chauncey/test/read`

#### -tail 

`hdfs dfs -tail /user/chauncey/test/read`	

#### -test

一样的,显示文本文件的内容

#### -chgrp -chmod -chown

`hdfs dfs -chown root /user/chauncey/test/read`

`hdfs dfs -chown chauncey /user/chauncey/test/read`

#### -copyFromLocal

相当于-put, `hdfs dfs -copyFromLocal dfdfdf.zip /user/chauncey/test`

#### -copyToLocal

`hdfs dfs -copyToLocal /user/chauncey/test/dfdfdf.zip ./`

#### -cp

在hdfs 中复制, `hdfs dfs -cp /user/chauncey/test/dfdfdf.zip /user/chauncey`

#### -mv



#### -get

等同于copyToLocal

#### -getmerge



#### -put

等同于copyFromLocal

#### -rm

`hdfs dfs -rm -r /user/chauncey/test` 递归删除

#### -rmdir



#### -df

`hdfs dfs -df -h`

#### -du

`hdfs dfs -du -h /user`

#### -count

`hdfs dfs -count -h /`

#### -setrep

### 通过API 操作HDFS

#### 1, 文件上传

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HDFSClient {
    public static void uploadFIle(String[] args) throws Exception{
    	Configuration conf = new COnfiguration();
    	conf.set("fs.defaultFS", "hdfs://hadoop-1:9000");
    	FileSystem fs =  FileSystem.get(conf);
    	fs.copyFromLocalFile(new Path("/home/chauncey/Desktop/tt.deb"),new Path("/test"));
    	fs.close();
    }
}
```



```java
public class HDFSClient {
    public static void uploadFIle(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"),conf, "root");
        fs.copyFromLocalFile(new Path("/home/chauncey/Desktop/tt.deb"),new Path("/test"));
        fs.close();

    }
}
```



#### 2, 获取文件系统

```java
    public static void getFIleSystem() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
        System.out.println(fs.toString());
    }
```



#### 3, 文件下载

```java
public static void getFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");

    fs.copyToLocalFile(new Path("/test/tt.deb"), new Path("/home/chauncey/Desktop/ttt.deb"));
}
```

#### 4, 目录创建

```java
public static void mkDir() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
    fs.mkdirs(new Path("/test/tiantant/s/q/t"));
    fs.close();
}
```

#### 5, 文件夹删除

```java
public static void deleteAtHDFS() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
    fs.delete(new Path("/test/tt.deb"), true);
    fs.close();
}
```

#### 6, 文件名更改

```java
public static void renameATHDFS() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
    fs.rename(new Path("/test/t.deb"), new Path("/test/r.deb"));
    fs.close();
}
```

#### 7, 文件详情查看

```java
public static void readFileAtHDFS() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");

    RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/test"), true);

    while (listFiles.hasNext()) {
        LocatedFileStatus status = listFiles.next();
        System.out.println(status.getPath().getName() + " " + status.getBlockSize() / 1024 / 1024 + "MB");
        System.out.println(status.getLen() / 1024 / 1024);
        System.out.println(status.getPermission());
        BlockLocation[] blockLocations = status.getBlockLocations();

        for (BlockLocation bl : blockLocations) {
            System.out.println(bl.getOffset());
            String[] hosts = bl.getHosts();
            System.out.println(Arrays.toString(hosts));
        }
    }
}
```

#### 8, 文件夹查看

```java
public static void readFolderAtHDFS() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
    fs.listStatus(new Path("/test"));
    FileStatus[] listStatus = fs.listStatus(new Path("/test"));
    for (FileStatus status : listStatus) {
        if (status.isFile())
            System.out.println("f--: " + status.getPath().getName());
        else System.out.println("d--: " + status.getPath().getName());
    }
    fs.close();
}
```

### 通过IO 流操作

#### 上传

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class IOToHDFS {
public static void putFIleTOHDFS() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
    // fs.create
    FSDataOutputStream fos = fs.create(new Path("/test/output/spark-2.4.0-bin-hadoop2.7.tgz"));
    FileInputStream fis = new FileInputStream(new File("/usr/local/spark-2.4.0-bin-hadoop2.7.tgz"));
    try {
        IOUtils.copyBytes(fis, fos, conf);
    } catch (Exception e) {

    } finally {
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
}
```



#### 下载

```java
// 从hdfs 上下载, 需要用hdfs的输入流(inputStream)
public static void getFileFromHDFS() throws Exception {
    Configuration conf = new Configuration();

    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");
    FSDataInputStream fis = fs.open(new Path("/test/output/4.txt"));
    FileOutputStream fos = new FileOutputStream(new File("/usr/local/4.txt"));
    try {
        IOUtils.copyBytes(fis, fos, conf);
    } catch (Exception e) {

    } finally {
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
```

#### 大文件分块下载

```java
public static void getFileFromHDFSSeek1() throws Exception{
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf,"root");
    // download, fs.open()
    FSDataInputStream fis = fs.open(new Path("/test/output/spark-2.4.0-bin-hadoop2.7.tgz"));
    FileOutputStream fos = new FileOutputStream(new File("/home/chauncey/Desktop/spark-2.4.0-bin-hadoop2.7.tgz.part1"));
    byte[] buf = new byte[1024];
    for(int i = 0; i < 1024*128;i++){
        fis.read(buf);
        fos.write(buf);
    }
    try {
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }catch (Exception e){

    }
}
public static void getFileFromHDFSSeek2() throws Exception{
    // get File System
    Configuration conf = new Configuration();

    FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf,"root");

    // get input stream
    FSDataInputStream fis = fs.open(new Path("/test/output/spark-2.4.0-bin-hadoop2.7.tgz"));

    // get output stream
    FileOutputStream fos = new FileOutputStream(new File("/home/chauncey/Desktop/spark-2.4.0-bin-hadoop2.7.tgz.part2"));
    // join the streams
    fis.seek(1024*1024*128);
    IOUtils.copyBytes(fis, fos, conf);

    // close streams
    try{
        IOUtils.copyBytes(fis, fos, conf);

    }catch (Exception e){
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
```

### HDFS 数据流

#### 1 写过程

假如我有 1 个namenode, 3个datanode, 现在要上传一个200MB 的文件

1. 一个两百兆的文件上传到HDFS 上面, 因为一块是128, 所以要分成两块.

2. 向namenode 请求上传文件 /test/ss.avi, namenode 中有 metadata 元数据
3. 请求上传第一个 block (0 ~ 128MB), 请返回哪一个 datanode 
4. 返回 dn1, dn2, dn3, 表示采用这三个节点存储数据 (有备份)
5. 传输数据要请求建立block 传输通道, 就近原则, 看看哪一个datanode 更近, 比如dn1 更近, 先上传到dn1, dn1 再请求建立通道给dn2, dn2 请求建立通道dn3
6. 染回 dn3 应答成功, dn2 应答成功, dn1 应答成功, 
7. 开始上传, 先上传到缓冲区, 缓冲区往block 中写, dn1缓冲区向dn2缓冲区写, dn2缓冲区往dn3 的缓冲区写 

#### 1.1 网络拓扑概念

在海量数据处理中, 主要限制因素是节点之间数据传输速率,  

节点距离: 两个节点到达最近的共同祖先的距离之和 (到达最近的共同路由器)

distance(/d1/r1/n1, /d1/r1/n1) 同一节点上不同的进程

distance(/d1/r1/n1, /d1/r1/n2) 同一机架上不同的节点

distance(/d1/r1/n1, /d1/r3/n2) 同一个数据中心 不同机架上的节点

distance(/d1/r1/n1, /d=2/r4/n3) 不同数据中心的节点   

#### 1.2 机架感知

https://hadoop.apache.org/docs/r3.1.1/hadoop-project-dist/hadoop-common/RackAwareness.html

比如说我有4个datanode dn1 和dn2 是一个集群, dn3 和dn4 是一个集群

* 低版本hadoop 副本节点选择  

第一个副本在client 所处的节点上, 如果客户端在集群外, 随机选择一个

第二个副本和第一个副本处于不同机架的随机节点上

第三个副本和第二个副本位于相同机架, 不同节点

* **高版本**

第一个副本在client 所处的节点上, 如果客户端在集群外, 随机选择一个

第二个副本和第一个副本在相同机架, 不同节点

第三个副本在不同机架, 随机节点

```java
import org.apache.hadoop.net.DNSToSwitchMapping;
import java.util.ArrayList;
import java.util.List;

public class AutoRack implements DNSToSwitchMapping {
    public List<String> resolve(List<String> ips) {
        //ips : 1 hadoop-1 172.18.0.2

        ArrayList<String> lists = new ArrayList<String>();
        int ipnumber = 0;
        if (ips != null && ips.size() > 0) {
            for (String ip : ips) {

                if (ip.startsWith("hadoop")) {
                    String ipnum = ip.substring(6);
                    ipnumber = Integer.parseInt(ipnum);

                } else if (ip.startsWith("172")) {
                    int index = ip.lastIndexOf(".");
                    String ipnum = ip.substring(index + 1);
                    ipnumber = Integer.parseInt(ipnum);
                }
            }

            if (ipnumber < 5) {
                lists.add("/rack1/" + ipnumber);
            } else {
                lists.add("/rack2/" + ipnumber);
            }
        }
        return lists;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> names) {

    }
}
```



配置core-site.xml

```
# 默认的
<property>
<name>net.topology.node.switch.mapping.impl</name>
<value>org.apache.hadoop.net.ScriptBasedMapping</value>
</property>

# 配置后
<property>
<name>hdfs.AutoRack</name>
<value></value>
</property>
```



#### 2 读过程

读一个200MB的ss.avi

有一个namenode, 存储元数据, 有三个datanode

1. 请求下载 /test/ss.avi
2. 返回目标文件的元数据
3. 请求读数据 blk_1 
4. 可能是dn1传输数据到内存
5. 请求读数据blk_2
6. 可能是dn2传输数据到内存
7. 在内存中合并, 存储到本机上

#### 3 一致性模型

```java
    public static void writeFIle() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-1:9000"), conf, "root");

        Path path = new Path("hdfs://hadoop-1:9000/test/hello.txt");
//        FSDataOutputStream fos = fs.create(path,true);
//        fos.write("hello world1111".getBytes());
        FSDataOutputStream fos1= fs.append(path);
        fos1.write("hello world 112233".getBytes());
//        fos.hflush();
//        fos.close();//Failed to APPEND_FILE /test/hello.txt ,already the current lease holder.
        fos1.hflush();
        fos1.close();

    }
```

### namenode 和 secondarynamenode

node 每个block 元数据占 150 byte

Namenode会将HDFS的文件和目录元数据存储在一个叫fsimage的二进制文件中，每次保存fsimage之后到下次保存之间的所有hdfs操作，将会记录在editlog文件中，当editlog达到一定的大小（bytes，由fs.checkpoint.size参数定义）或从上次保存过后一定时间段过后（sec，由fs.checkpoint.period参数定义），namenode会重新将内存中对整个HDFS的目录树和文件元数据刷到fsimage文件中。Namenode就是通过这种方式来保证HDFS中元数据信息的安全性。

1. 第一次启动namenode 格式化后, 创建fsimage 和edits 文件, 如果不是第一次, 直接把他们加载到内存中
2. 元数据的增改删请求, 
3. namenode记录操作日志, 更新滚动日志edits
4. namenode内存数据增删改查

#### 2nn

1. 请求是否需要checkpoint (定时时间到, edits 中的数据满了)
2. secondare namenode请求执行checkpoint
3.   namenode 滚动正在写的edits日志
4. 将滚动前的edits 和镜像文件拷贝到 secondary namenode
5. secondary namenode 加载编辑日志和镜像文件到内存, 并合并
6. 生成新的镜像文件  fsimage.chkpoint
7. 拷贝fsimage.chkimage 到namenode
8. namenode 将fsimage.chkpoint 重新命名成fsimage

chkpoint 时间参数设置, 默认一小时执行一次

```xml
<property>
<name>dfs.namenode.checkpoint.period</name>
<value>3600</value> // 一分钟执行一次
</property>

<property>
  <name>dfs.namenode.checkpoint.txns</name>
  <value>1000000</value> // 分钟检查一次操作次数，当操作次数达到1百万时，SecondaryNameNode执行一次。
<description>操作动作次数</description>
</property>

<property>
  <name>dfs.namenode.checkpoint.check.period</name>
  <value>60</value>
<description> 1分钟检查一次操作次数</description>
</property>
```

#### hadoop 数据的存储

在自己定义的文件夹中, 有dfs, dfs 中有name 和data,

name 中有current, current 中有镜像文件和编辑日志

#### **dfs　下面有节点的文件夹**

namenode 格式化后, 将会在/hadoop_tmp/dfs/name/current 产生如下文件

edits_0000000000000000001-0000000000000000002

fsimage_0000000000000000079

fsimage_0000000000000000079

1. fsimage: HDFS 文件系统元数据的一个永久性的检查点, 其中包含hdfs 文件系统的所有目录和文件idnode 的序列化信息

2. edits: 存放hdfs 文件系统所有更新操作, 文件系统客户端执行的所有写操作首先会被记录到edits 文件中.
3. seen_txid 文件保存的是一个数字, 就是最后一个edits_数字
4. 每次namenode 启动的时候, 就会把fsimage 文件 读入内存, 并从00001 开始到seen_txid 记录的数字依次执行每个edits 里面的更新操作, 保证内存中元数据是最新的, 同步的 可以看成namenode 启动的时候就将fsimage 和edits 文件进行了合并

#### hdfs oiv

hdfs oiv  -p XML -i fsimage_0000000000000000081 -o ./fsimage.xml

hdfs oev  -p XML -i edits_0000000000000000037-0000000000000000079 -o ./edits.xml

* hdfs dfs -put edits.xml  /test 之后产生的操作记录

```xml
<RECORD>
    <OPCODE>OP_ADD</OPCODE>
    <DATA>
      <TXID>91</TXID>
      <LENGTH>0</LENGTH>
      <INODEID>16403</INODEID>
      <PATH>/test/edits.xml._COPYING_</PATH>
      <REPLICATION>2</REPLICATION>
      <MTIME>1550923904505</MTIME>
      <ATIME>1550923904505</ATIME>
      <BLOCKSIZE>134217728</BLOCKSIZE>
      <CLIENT_NAME>DFSClient_NONMAPREDUCE_735434787_1</CLIENT_NAME>
      <CLIENT_MACHINE>172.18.0.2</CLIENT_MACHINE>
      <OVERWRITE>true</OVERWRITE>
      <PERMISSION_STATUS>
        <USERNAME>root</USERNAME>
        <GROUPNAME>supergroup</GROUPNAME>
        <MODE>420</MODE>
      </PERMISSION_STATUS>
      <ERASURE_CODING_POLICY_ID>0</ERASURE_CODING_POLICY_ID>
      <RPC_CLIENTID>fe0c0593-2339-4371-b773-1a5aba37c523</RPC_CLIENTID>
      <RPC_CALLID>3</RPC_CALLID>
    </DATA>
  </RECORD>
  <RECORD>
    <OPCODE>OP_ALLOCATE_BLOCK_ID</OPCODE>
    <DATA>
      <TXID>92</TXID>
      <BLOCK_ID>1073741835</BLOCK_ID>
    </DATA>
  </RECORD>
  <RECORD>
    <OPCODE>OP_SET_GENSTAMP_V2</OPCODE>
    <DATA>
      <TXID>93</TXID>
      <GENSTAMPV2>1013</GENSTAMPV2>
    </DATA>
  </RECORD>
  <RECORD>
    <OPCODE>OP_ADD_BLOCK</OPCODE>
    <DATA>
      <TXID>94</TXID>
      <PATH>/test/edits.xml._COPYING_</PATH>
      <BLOCK>
        <BLOCK_ID>1073741835</BLOCK_ID>
        <NUM_BYTES>0</NUM_BYTES>
        <GENSTAMP>1013</GENSTAMP>
      </BLOCK>
      <RPC_CLIENTID/>
      <RPC_CALLID>-2</RPC_CALLID>
    </DATA>
  </RECORD>
 <RECORD>
    <OPCODE>OP_CLOSE</OPCODE>
    <DATA>
      <TXID>95</TXID>
      <LENGTH>0</LENGTH>
      <INODEID>0</INODEID>
      <PATH>/test/edits.xml._COPYING_</PATH>
      <REPLICATION>2</REPLICATION>
      <MTIME>1550923904645</MTIME>
      <ATIME>1550923904505</ATIME>
      <BLOCKSIZE>134217728</BLOCKSIZE>
      <CLIENT_NAME/>
      <CLIENT_MACHINE/>
      <OVERWRITE>false</OVERWRITE>
      <BLOCK>
        <BLOCK_ID>1073741835</BLOCK_ID>
        <NUM_BYTES>2704</NUM_BYTES>
        <GENSTAMP>1013</GENSTAMP>
      </BLOCK>
      <PERMISSION_STATUS>
        <USERNAME>root</USERNAME>
        <GROUPNAME>supergroup</GROUPNAME>
        <MODE>420</MODE>
      </PERMISSION_STATUS>
    </DATA>
  </RECORD>
  <RECORD>
    <OPCODE>OP_RENAME_OLD</OPCODE>
    <DATA>
      <TXID>96</TXID>
      <LENGTH>0</LENGTH>
      <SRC>/test/edits.xml._COPYING_</SRC>
      <DST>/test/edits.xml</DST>
      <TIMESTAMP>1550923904652</TIMESTAMP>
      <RPC_CLIENTID>fe0c0593-2339-4371-b773-1a5aba37c523</RPC_CLIENTID>
      <RPC_CALLID>8</RPC_CALLID>
    </DATA>
  </RECORD>
```

#### 滚动编辑日志

hdfs dfsadmin -rollEdits

#### 版本号

在current 目录下, 有一个VERSION

```
namespaceID=1538173186
clusterID=CID-eb11340b-1974-408b-9e07-c0466c2eba49
cTime=1550795843992
storageType=NAME_NODE
blockpoolID=BP-893578031-172.17.0.2-1550795843992
layoutVersion=-64
```

namespaceID 区分namenode

clusterID 

cTime namenode 存储系统的创建时间, 刚刚格式化的,这个属性为0

#### SecnodaryNameNode 目录结构

hadoop_tmp/dfs/namesecondary/current

SechndaryNameNode 的namesecondary/current 目录和主nomenode 的current 目录布局相同

好处: 1. 在namenode 发生故障时, 可以从2nn 恢复数据    

方法1: 将2nn 中数据拷贝到namenode 存储数据的目录

方法2: 使用-importCheckpoint 选项启动namenode 守护进程, 从而将2nn 用作新的主namenode

#### 模拟namenode 故障1

jps 

1. kill -9 namenode  PID

2. 删除namenode 数据 ` rm -rf /opt/module/hadoop-2.7.2/data/tmp/dfs/name/*`

3. 拷贝SecondaryNameNode, 

4. 重新启动namenode `sbin/hadoop-daemon.sh start namenode`  `hdfs --daemon start`

#### 模拟namenode 故障2

1. 修改hdfs-site.xml

   ```xml
   <property>
     <name>dfs.namenode.checkpoint.period</name>
     <value>120</value>
   </property>
   
   <property>
     <name>dfs.namenode.name.dir</name>
     <value>/opt/module/hadoop-2.7.2/data/tmp/dfs/name</value>
   </property>
   ```

2. kill -9 namenode进程

3. 删除name 文件中内容  `rm -rf /opt/module/hadoop-2.7.2/data/tmp/dfs/name/*`

4. 如果SecondaryNameNode 不和namenode 在一个主机节点中, 需要将namesecondary 文件夹放在dfs 文件夹中, 

5. 启动     sbin/hadoop-daemon.sh start namenode

6. 可以删除in_use.lock

#### 集群安全模式操作

Namenode启动时，首先将映像文件（fsimage）载入内存，并执行编辑日志（edits）中的各项操作。一旦在内存中成功建立文件系统元数据的映像，则创建一个新的fsimage文件和一个空的编辑日志。此时，namenode开始监听datanode请求。但是此刻，namenode运行在安全模式，即namenode的文件系统对于客户端来说是只读的。

系统中的数据块的位置并不是由namenode维护的，而是以块列表的形式存储在datanode中。在系统的正常操作期间，namenode会在内存中保留所有块位置的映射信息。在安全模式下，各个datanode会向namenode发送最新的块列表信息，namenode了解到足够多的块位置信息之后，即可高效运行文件系统。

如果满足“最小副本条件”，namenode会在30秒钟之后就退出安全模式。所谓的最小副本条件指的是在整个文件系统中99.9%的块满足最小副本级别（默认值：dfs.replication.min=1）。在启动一个刚刚格式化的HDFS集群时，因为系统中还没有任何块，所以namenode不会进入安全模式。

命令:

1. bin/hdfs dfsadmin -safemode get
2. bin/hdfs dfsadmin -safemode enter
3. bin/hdfs dfsadmin -safemode leave
4. bin/hdfs dfsadmin -safemode wait

当处于安全模式时, 不能写, 如果有任务, 无能完成, 要求安全模式结束后, 立即执行  wait

案例:

1. 进入安全模式 bin/hdfs dfsadmin -safemode enter

2. 执行 一个脚本 

   ```shell
   #!/bin/bash
   bin/hdfs dfsadmin -safemode wait
   bin/hdfs dfs -put ~/hello.txt /root/hello.txt
   ```

3. 再打开一个窗口，执行 `bin/hdfs dfsadmin -safemode leave`

### datanode

#### 工作机制

里面有块, 块中有 数据, 数据长度, 校验和, 时间戳

1. datanode 启动后向namenode 注册

2. 注册成功

3. 以后每一个小时上报所有块信息

4. 心跳每3秒一次, 心跳返回结果带有namenode 给datanode 的命令

5. 超过10分钟另30秒 没有收到datanode2 心跳, 则认为该节点不可用

   ```xml
   <property>
       <name>dfs.namenode.heartbeat.recheck-interval</name>
       <value>300000</value>
   </property>
   <property>
       <name> dfs.heartbeat.interval </name>
       <value>3</value>
   </property>
   ```

   timeout : 计算公式=2 * dfs.namenode.heartbeat.recheck-interval + 10 * dfs.heartbeat.interval, 默认300000ms为5分钟, 心跳3秒

#### 新增datanode节点

1. 在hdfs-site.xml 配置文件中增加dfs.hosts,并写上所有主机名称

```xml
<property>
<name>dfs.hosts</name>
      <value>/opt/module/hadoop-2.7.2/etc/hadoop/dfs.hosts</value>
</property>
```

2. 刷新namenode   hdfs dfsadmin -refreshNodes
3. 刷新resourcemanager    yarn rmadmin -refreshNodes
4. 在workers 文件增加主机名称
5. 在新增节点单独命令启动新的数据节点和节点管理器 `sbin/hadoop-daemon.sh start datanode`   `sbin/yarn-daemon.sh start nodemanager`
6. 如果数据不均衡  `sbin/start-balancer.sh`

#### 退役旧的数据节点

1. 在namenode 的 etc/hadoop 目录中创建 dfs.hosts.exclude 文件, 写入需要退役的节点主机名称,和dfs.hosts 不一样

2. 在hdfs-site.xml 配置文件中增加dfs.hosts.exclude 属性

   ```xml
   <property>
   <name>dfs.hosts.exclude</name>
         <value>/opt/module/hadoop-2.7.2/etc/hadoop/dfs.hosts.exclude</value>
   </property>
   ```

3. 刷新namenode 和resourcemanager  `hdfs dfsadmin -refreshNodes`    `yarn rmadmin -refreshNodes`

4. 检查web 浏览器, 退役的节点状态为decommission in progress, 等到状态为decommissioned, 到节点上 `sbin/hadoop-daemon.sh stop datanode`   `sbin/yarn-daemon.sh stop nodemanager`

5.  从dfs.hosts.exclude 文件中删除退役节点, 从dfs.hosts 文件中删除退役节点, 

6. 再刷新namenode 和 resourcemanager    `hdfs dfsadmin -refreshNodes`    `yarn rmadmin -refreshNodes`

7. 从workers 文件中删除退役节点

8. 如果数据不均衡 可以使用命令  `sbin/start-balancer.sh `

### 其他功能

#### 集群之间数据拷贝

1. 用scp ir hello.txt root@hadoop103:/user/atguitu/hello.txt
2. scp -r root@hadoop103:/user/atguigu/hello.txt  hello.txt

使用discp 

bin/hadoop distcp hdfs://haoop102:9000/user/atguigu/hello.txt hdfs://hadoop103:9000/user/atguigu/hello.txt

#### 存档

存储小文件非常低效,因为 在内存中存储元数据消耗内存

但是一个1MB的文件以大小为128MB的块存储，使用的是1MB的磁盘空间，而不是128MB。

Hadoop存档文件或HAR文件，是一个更高效的文件存档工具，它将文件存入HDFS块，在减少namenode内存使用的同时，允许对文件进行透明的访问。具体说来，Hadoop存档文件可以用作MapReduce的输入。

1. start-yarn.sh
2. hadoop archive -archiveName myhar.har -p /test/small   /test/my
3. 查看    hadoop fs -lsr /test/my/myhar.har      hadoop fs -ls -R har:///test/my/myhar.har

#### 快照管理

快照相当于对目录做一个备份, 不会立即复制所有文件, 而是指向同一个文件, 写入时, 产生新文件

1. hdfs dfsadmin -allowSnapshot 路径  (开启快照功能)
2. hdfs dfsadmin -disallowSnapshot 路径 (禁用指定目录的快照功能, 默认)
3. hdfs dfs -createSnapshot 路径 (创建快照)
4. hdfs dfs -createSnapshot 路径 名称 (指定名称创建快照)
5. hdfs dfs -renameSnapshot 路径 旧名称  新名称(重命名)
6. hdfs lsSnapshottableDir (列出当前用户所有快照目录)
7. hdfs snapshotDiff 路径1 路径2 (比较两个快照目录的不同)
8. hdfs dsf -deleteSnapshot <Path> <snapshotname> 删除快照

（1）开启/禁用指定目录的快照功能

hdfs dfsadmin -allowSnapshot /user/atguigu/data		

hdfs dfsadmin -disallowSnapshot /user/atguigu/data	

​	（2）对目录创建快照

hdfs dfs -createSnapshot /user/atguigu/data		// 对目录创建快照

通过web访问hdfs://hadoop102:9000/user/atguigu/data/.snapshot/s…..// 快照和源文件使用相同数据块

hdfs dfs -lsr /user/atguigu/data/.snapshot/

​	（3）指定名称创建快照

hdfs dfs -createSnapshot /user/atguigu/data miao170508		

​	（4）重命名快照

hdfs dfs -renameSnapshot /user/atguigu/data/ miao170508 atguigu170508		

​	（5）列出当前用户所有可快照目录

hdfs lsSnapshottableDir	

​	（6）比较两个快照目录的不同之处

hdfs snapshotDiff /user/atguigu/data/  .  .snapshot/atguigu170508	

​	（7）恢复快照

hdfs dfs -cp /user/atguigu/input/.snapshot/s20170708-134303.027 /user

#### 回收箱

1. 

默认值fs.trash.interval=0，0表示禁用回收站，可以设置删除文件的存活时间。

默认值fs.trash.checkpoint.interval=0，检查回收站的间隔时间。

要求fs.trash.checkpoint.interval<=fs.trash.interval。

2. 启用回收箱

修改core-site.xml，配置垃圾回收时间为60分钟。

```xml
<property>
    <name>fs.trash.interval</name>
    <value>60</value>
</property>
```

 3 查看回收箱 /user/username/.Trash/

4 修改访问垃圾回收站用户名称

默认 dr.who, 修改为root

```xml
<property>
  <name>hadoop.http.staticuser.user</name>
  <value>atguigu</value>
</property>
```

5. 通过程序删除的文件不会经过回收站, 需要调用moveToTrash() 才进入回收箱

Trash trash = new Trash(conf);

trash.moveToTrash(path);

6 恢复回收箱数据

hadoop fs -mv /user/atguigu/.Trash/Current/user/atguigu/input    /user/atguigu/input

7 清空回收箱

hdfs dfs -expunge

### namenode 优化

#### 1 namenode 元数据备份使用固态硬盘

#### 2 为namenode 指定多个元数据目录 

使用`dfs.name.dir` 或者 `dfs.namenode.name.dir	 `制定,

#### 3 设置 dfs.namenode.name.dir.restore 为true

#### 4 namenode 必须设置为raid 1

有镜像盘

#### 5 namenode 日志目录有足够空间

#### 6 提高吞吐量(位宽)

### hdfs 优化

#### 1 保证rpc 调用会有较多线程数

dfs.namenode.handler.count  默认10, 调整为  50 -100

dfs.datanode.handler.count 默认10,  5 ~10

#### 副本

dfs.replication 一般2~3, 如果数据很重要 3~5

#### 文件块

dfs.blocksize  如果大量单个文件小于100M, 设置64M, 如果达到GB 设置256M