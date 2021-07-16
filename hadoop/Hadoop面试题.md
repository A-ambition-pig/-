# Hadoop面试题

### 1、常用端口号

| 端口名称                   | Hadoop 2.x | Hadoop 3.x     |
| -------------------------- | ---------- | -------------- |
| Namenode 内部通信端口      | 8020/9000  | 8020/9000/9820 |
| Namenode Http UI           | 50070      | 9870           |
| MapReduce 查看执行任务端口 | 8088       | 8088           |
| 历史服务器通信端口         | 19888      | 19888          |

### 2、常用配置文件

core-site.xml、hdfs-site.xml、yarn-site.xml、mapred-site.xml、workers (3.x)