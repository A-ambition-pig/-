---
typora-root-url: ./
---

# MapReduce

分布式计算框架

优点：1、易于编程 2、良好扩展性 3、高容错性 4、适合PB级以上海量数据的离线处理

缺点：1、不擅长实时计算 2、不擅长流失计算 3、不擅长DAG计算



**WordCount**

![](\MapReduce_WordCount.png)



### MapReduce框架原理

![](\MapReduce_框架原理.png)

#### FileInputFormat数据输入

首先，需要解决的是，一个job会生成几个Task去执行？

##### 切片与MapTask并行度决定机制

MapTask的并行度决定Map阶段的任务处理并发度，进而影响到整个Job的处理速度。

思考：MapTask并行度是由什么决定的？

**数据块与数据切片**

数据块：Block是HDFS把数据一块一块存储在物理机器上。

数据切片：在逻辑上对输入进行切片，并不会在磁盘上将其切分成片进行存储。**数据切片是MapReduce程序计算输入数据的单位。**一个切片会对应启动一个MapTask。

![](\数据切片与MapTask并行度.png)

###### 切片源码

（1）程序先找到数据存储的目录

（2）开始遍历处理目录下的每一个文件

（3）遍历：

​	a）获取文件大小sizeOf(xxx.txt)

​	b）计算切片大小

​		computeSplitSize（Math.max(**minSize**, Math.min(**maxSize**, blocksize)）) == blocksize =128M

​	c）默认情况下，切片大小=blocksize

​	d）开始切，**<u>注意：每次切完后，都要判断剩下的部分是否大于块的1.1倍，不大于就划分为同一个切片</u>**

​	e）将切片信息写入到 切片规划文件

（4）提交切片规划文件到Yarn中，MrAppmaster开始计算MapTask个数。

说明：整个切片过程都在getSplit()中完成；InputSplit只记录了切片的元数据信息，比如起始位置、长度以及所在的节点列表等。





##### Job提交流程

![](\Job提交流程.png)

建立连接：

(1) waitForCompletion => submit =>  创建提交Job的代理;判断是本地运行环境(LocalJonRunner)还是yarn集群运行环境(YarnRunner)



提交Job：

（1）创建给集群提交数据的stagingDir

（2）获取jobid，并创建job路径

（3）拷贝jar包到集群（区分是否是yarn集群环境，本地不用）

（4）计算**切片**，生成切片规划文件

（5）向Stag路径写入XML配置文件

（6）提交Job，返回提交状态



##### FileInputFormat机制

###### TextIntputFormat

TextInputFormat是默认的FileInputFormat实现类。

按行读取每条记录。k是刚行存储在整个文件中的起始字节偏移量，LongWritable类型；v是这行内容，Text类型。



**切片公式**：
$$
computeSplitSize(Math.max(minSize, Math.min(maxSize, blocksize)))
$$
默认：minSize = 1，maxSize = Long.MAX_VALUE

即，默认情况，切片大小=blocksize=128M

###### CombineTextInputFormat

由于TextInputFormat对小文件的处理效率低下，CombineTextFileInputFormat用于处理小文件过多的场景。

它将多个小文件从**逻辑上划分**到一个切片中，这样多个小文件可以交给一个MapTask处理。

CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);//4M

注意：虚拟存储切分最大值设置最好根据实际的小文件大小情况来设置。

![CombineTextInputFormat-处理小文件](\CombineTextInputFormat-处理小文件.png)





#### MapReduce工作流程

![MapReduce工作流程-1](\MapReduce工作流程-1.png)

![MapReduce工作流程-2](\MapReduce工作流程-2.png)



注意：

（1）Shffle中的缓冲区大小会影响到MapReduce程序的执行效率，原则上，缓冲区越大，磁盘io的次数越少，执行速度就越快。

（2）缓冲区的大小可以通过参数调整，参数：mapreduce.task.io.sort.mb默认 100M

##### Shuffler机制

![](\Shuffler机制.png)



##### Partition分区

（1）如果ReduceTaskNums > getPartitionsNums，则会多产生几个空的输出文件

（2）如果1 < ReduceTaskNums < getPartitionsNums，则有一部分分区数据无处安放，会Exception

（3）如果 ReduceTaskNums =1 ，则不管MapTask端输出多少个分区文件，最终结果都交给同一个ReduceTask。

（4）分区号必须从0开始，逐一累加



##### MapTask工作机制

![](/MapTask工作机制.png)





#### FileOutputFormat数据输出

默认输出：TextOutputFormat







### 传输相关

#### 序列化

把内存中的对象，转换成字节序列，以便于存储到磁盘(持久化)和网络传输。

为什么不用Java序列化？

Java的序列化是一个重量级序列化框架(Serializable)，一个对象被序列化后，会附带很多额外的信息，不便于传输。



#### 压缩
