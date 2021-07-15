# zookeeper

学习之前，几个问题？

1、Zookeeper是什么，设计出它的目的是什么？

Zookeeper，分布式协调服务框架

2、为了实现它所解决的问题，采取了哪些技术手段

观察者设计模式



**特点**

1、集群里，1个Leader，多个Follower

2、半数节点以上，集群正常提供服务。适合奇数台

3、全局数据一致，每个server保存一份相同replicas

4、更新请求顺序执行

5、数据更新原子性

6、实时性，在一定范围内，Client能读到最新数据



对于以上特点，Zookeeper是如何保证的？



#### **<u>Zookeeper 选举机制</u>**（面试重点）

###### 第一次启动

服务器初始化启动

每一台服务器启动，都会重新投一票，但是会根据SID投给SID最大的节点

超过一半即为Leader



###### 非第一次启动

服务器运行期间无法和Leader保持连接

1）集群存在Leader情况，此时选举会被告知Leader情况，重新建立连接

2）集群Leader挂了，

选举机制：A.EPOCH大的直接胜出 B. EPOCH相同，事务id（ZXID）大的胜出 C.ZXID相同，SID大的胜出





### 客户端

#### 节点类型

持久persistent、短暂ephemeral、有序号sequential、无序号



### 监听器原理

![监听器原理](E:\github\BigDataLearning\zookeeper\监听器原理.png)

常见监听

1）节点数据变化

get path [watch]

2）子节点增减变化

ls path [watch]

### 读写流程

##### 写流程

1)Client -> Leader

![write_client_Leader](E:\github\BigDataLearning\zookeeper\write_client_Leader.png)

2)CLient -> Follower

![write_client_Follower](E:\github\BigDataLearning\zookeeper\write_client_Follower.png)

#### 案例

##### 1、服务器动态上下线监听

![服务器动态上下线](E:\github\BigDataLearning\zookeeper\服务器动态上下线.png)

服务端

```
1、获取zk连接
2、注册服务器到集群
3、启动业务逻辑（sleep）
```

客户端

```
1、获取服务器子节点，并且对父节点进行监听
2、存储服务器信息列表
3、遍历所有节点，获取节点中的主机名称信息
4、打印服务器列表信息
```



##### 2、分布式锁

![分布式锁原理](E:\github\BigDataLearning\zookeeper\分布式锁原理.png)

1）Zookeeper实现

2)	Curator框架实现（生产环境）



https://zookeeper.apache.org/  