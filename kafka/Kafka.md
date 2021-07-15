# Kafka

学习之前，kafka是什么？做什么用的？

1、Kafka是做什么用的？为了解决什么问题？

2、用了哪些技术去解决了哪些问题？



消息队列两种模式

1）点对点模式

2）发布/订阅模式

a.生产者 推送

b.消费者 拉取 kafka采取



### Kafka架构



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

**多少个follower同步完成之后发送ack？**

1）副本数据同步策略

a.半数以上

b.全部同步，才发送ack。kafka选择这个，为什么？

```

```

2）ISR(in-sync replica set)

同步队列

通信时间replica.lag.time.max.ms、数据量-条数replica.lag.time.max.messages（高版本去掉了，0.9）

```
为什么去掉replica.lag.time.max.messages？

生产者，batch发送，条数较多,低版本会频繁操作zk，isr
```

3）ack应答机制

提供三种级别，0、1容易造成数据丢失

| acks-level | ISR副本等待                                                  |
| ---------- | ------------------------------------------------------------ |
| 0          | 不等，就返回ack。                                            |
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



#### 消费者



### Kafka命令

#### 主题管理

```powershell
kafka-topics.sh --zookeeper linux1:2181/myKafka --list
```



#### 生产者

```shell
kafka-console-producer.sh --topic first --broker-list linux1:9092
```



#### 消费者

偏移量保存

0.9版本之前，offset存在zookeeper，参数可为--zookeper

```shell
kafka-console-consumer.sh --topic first --bootstrap-server linux1:9092
```

