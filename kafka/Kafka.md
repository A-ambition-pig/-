---
typora-root-url: ./
---

# Kafka

学习之前，问几个问题？

1、Kafka是什么？设计出来为了解决那些问题？

2、Kafka是如何设计的，以及是怎么解决相关问题？



Kafka是**发布/订阅模式**的**消息队列**(Message Queue)，主要应用大数据实时处理领域

MQ传统应用场景：异步处理

消息队列好处：解耦、可恢复性、缓冲、灵活性&峰值处理能力、异步通信

MQ两种模式：点对点模式、发布/订阅模式（消费者拉、队列推）

a.生产者 推送

b.消费者 拉取 kafka采取



### Kafka架构

![](/Kafka架构.png)

#### 文件存储

.index文件

.log文件



#### 生产者

##### 分区策略

**为什么要分区？**

1）方便扩展  2）提高并发

###### 分区原则

**数据怎么发送？**

1）指定分区

2）没有指定分区，有key

3）都没有指定，round-robin算法



##### 数据可靠性保证

**kafka怎么确认数据成功发送？**

ack

**何时发送ack？**

确保有follower与leader同步完成，leader才发送ack，这样才能保证leader挂掉后能在follower中选取出新的leader

**多少个follower同步完成之后发送ack？**

方案一：半数以上的follower同步完成

方案二：全部follower同步完成



1）副本数据同步策略

a.半数以上

b.全部同步，才发送ack。kafka选择这个，为什么？

| 方案             | 优点                                     | 缺点                                      |
| ---------------- | ---------------------------------------- | ----------------------------------------- |
| 半数以上同步完成 | 延迟低                                   | 选举新的leader，容忍n台故障，需要2n+1副本 |
| 全部同步完成     | 选举新的leader，容忍n台故障，需要n+1副本 | 延迟高                                    |

kafka之所以选择第二种方案，因为为了同样容忍n台节点故障，方案一需要2n+1个副本，第二个方案只需要n+1个副本，kafka每个分区有大量数据，第一种会造成大量数据冗余。网络延迟在这种情况下对kafka的影响比较小



2）ISR(in-sync replica set)

为什么提出ISR？

leader收到数据后开始同步，但是有一个follower出现故障，会导致leader一直等待下去。为了解决这种问题，leader维护了一个同步队列ISR，意为和leader保持同步的follower集合。

当follower长时间没有与leader同步，则被提出ISR。

通信时间replica.lag.time.max.ms、数据量-条数replica.lag.time.max.messages（高版本去掉了，0.9）

```
为什么去掉replica.lag.time.max.messages？

生产者，batch发送，条数较多,低版本会频繁操作zk，isr
```

3）ack应答机制

对于某些不太重要的数据，对数据可靠性的要求不是很高，能够容忍少量数据丢失，所以没必要等ISR中的follower全部接受成功。

提供三种可靠性级别，0、1容易造成数据丢失

| acks-level | ISR副本等待                                                  |
| ---------- | ------------------------------------------------------------ |
| 0          | producer发送完，不等待broker的ack。                          |
| 1          | 等待leader同步完，不等follower。                             |
| -1（all）  | 等待Leader,follower全部同步完成。会造成**数据重复**(同步完成后，Leader挂掉)，也可能数据丢失（极限：ISR只有Leader） |

4）故障处理细节-**保证消费者消费的一致性**

Log文件中的HW和LEO

LEO：每个副本的最后一个offset

HW：ISR所有副本中最小的LEO，**消费者能见到**的最大offset

a.follower故障

b.leader故障

新leader成功选出，其余follower会截取掉HW之前的数据

**<u>注意</u>**：**只能保证ISR副本之间的数据一致性，并不能保证数据不丢失或者不重复**（ack保证）



##### Exactly Once语义

At Least Once、 At Most Once

0.11版本，引入幂等性 -> 数据重复性问题
$$
At Least Once + 幂等性 = Exactly Once
$$
启用幂等性，Producer参数，enable.idompotence=true

开启幂等性的Producer在初始化的时候，分配PID，发往同一Partition的消息会附带Sequence Number。Broker端会对<PID,Partition, SeqNumber>做缓存，从而保证Broker只会持久化一条。

但是，当Producer挂掉，重启后PID会变化，PartitionId也会变化，再次发送仍会有数据重复。

在这里，只能解决单分区单会话的ExactlyOnce，但是为了解决跨分区跨会话，又提出了什么解决方案？



#### 消费者

##### 消费方式

consumer 采用pull (拉) 模式从broker中读取数据



##### 分区分配策略

有消费者组才会有分区的概念

一个 consumer group有多个consumer，一个topic有多个partiiton，所以涉及到partition的分配问题，即哪个partition由哪个consumer来消费。

Kafka两种策略，RoundRobin、Range

###### 1）RoundRobin

按组来划分

###### 2）Range

默认。按主题来划分，考虑谁订阅了这个主题，再考虑组

什么时候触发这个策略执行？

消费者组添加、减少消费者时，增加到多于分区个数时，依旧重新分配

##### offset的维护

group+topic+partition，决定唯一offset

##### 消费者组消费



#### Kafka高效读取数据

多台快，因为有分区，提高并行度

单台快，因为顺序写磁盘、零拷贝技术

##### 顺序写磁盘

producer生产数据，写入log文件，写的过程一直追加到文件末端，物理端磁针按着磁道顺序写

##### 零拷贝技术

![](/零拷贝技术.png)

直接跟os系统打交道



#### Zookeeper在Kafka中的作用



#### Kafka事务

0.11引入事务支持，解决幂等性单分区单会话Exactly Once

##### Producer事务

引入全局唯一的TransactionID(客户端给)，并将Producer的PID与TransactionID绑定，实现**跨分区跨会话**的事务。

新组件Transaction Coordinator管理Transaction

##### Consumer事务

事务保证相对较弱



### Kafka命令

#### 主题管理

```powershell
kafka-topics.sh --zookeeper linux1:2181/myKafka --list
```



#### 生产者

```shell
kafka-console-producer.sh --topic first --broker-list linux1:9092
```

##### 消息发送流程

异步发送，main线程和Sender线程，以及一个线程共享变量RecordAccumulator

![](/producer发送消息.png)

相关参数：
batch.size： 只有数据积累到 batch.size 之后， sender 才会发送数据。
linger.ms： 如果数据迟迟未达到 batch.size， sender 等待 linger.time 之后就会发送数据。  

#### 消费者

偏移量保存

0.9版本之前，offset存在zookeeper，参数可为--zookeper

```shell
kafka-console-consumer.sh --topic first --bootstrap-server linux1:9092
```

