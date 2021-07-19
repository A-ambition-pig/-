# HDFS

HDFS（Hadoop Distributed File System），它是一个文件系统，用于存储文件，通过目录树来定位文件  

HDFS 的使用场景：适合一次写入，多次读出的场景。  



优点：高容错性、适合处理大数据、可构建在廉价机器上

缺点：不适合低延迟数据访问、无法高效的对大量小文件进行存储、不支持并发写入/文件修改

### 组成架构



1）NameNode(nn):Master

​	(1) 管理

2）DataNode(dn):





##### hdfs文件块

