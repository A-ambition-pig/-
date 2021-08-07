# ElasticSearch

一个高度可伸缩的开源全文搜索引擎

快速、实时地存储、搜索和分析大量数据

基于Lucence的搜索服务器

**概念**：

近实时(Near Realtime /NRT)

集群(Cluster): 唯一名称标识，正常情况只有一个master。注意，通信环境较差时，会出现多个master。

节点(Node):UUID

索引(Index)、文档(Doucument)、字段|属性(Filed)

| 数据库管理系统 | MySQL  | ES（5.x） | ES（6.x）                                  | ES（7.x）         |
| -------------- | ------ | --------- | ------------------------------------------ | ----------------- |
| 数据库         | DB     | Index     |                                            |                   |
| 表             | Table  | Type      | Index（json记录文档集合）/Type（逐渐淡化） | Index（移除Type） |
| 记录--行       | Row    | Document  | Document                                   | Document          |
| 字段--列       | Column | Field     | Field                                      | Field             |

分片(Shards)&副本(Replicas): 默认情况下，一个主分片和一个副本（7.x）。5.x，5分区，0副本。6.x，5分区，1副本。



### ElasticSearch RestfulAPI（DSL）

DSL：Domain Specific Language  

