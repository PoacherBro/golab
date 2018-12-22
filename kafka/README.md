# Kafka组件使用

## 依赖  
1. [Shopify/sarama](https://github.com/Shopify/sarama)  
2. [bsm/sarama-cluster](https://github.com/bsm/sarama-cluster)  

## 使用

参考 `producer_test.go` 和 `consumer_test.go`。  

## 本地测试

1. 在本地启动Kafka并且创建测试用的Topic，当前测试代码Topic为`test1`，参考官网[Kafka - QuickStart](https://kafka.apache.org/quickstart)进行配置；  
2. 运行命令启动Producer:`go test producer_test.go -count=1 -v`；  
3. 运行命令启动**多个**Consumer，与上面命令一样，修改脚本名字；  
4. 通过[gops](https://github.com/google/gops)查看运行的go进程，并且看到对应的PID；  
5. 找到一个Consumer的PID，通过命令停止此Consumer实例：`kill -s INT <PID>`。  

通过上述步骤可以查看Consumer加入或退出时Rebalance的情况。  


## 注意点

### 1. Producer的`push`方法使用问题

对于 Producer 的 `Push(topic string, msg []byte)` 方法，如果配置的是 `PartitionerHash`，则所有发送的消息都会放入一个 partition 里，达不到 Kafka 负载均衡的目的，影响性能。 
所以，对于 `PartitionerHash` 的发送方式，最好使用 `PushWithKey(topic, key string, msg []byte)` 方法，同时自己注意好 key 值的设计。  

### 2. Consumer的rebalance引起的问题

对于Kafka来说，有consumer新加入或者退出consumer group，都会导致rebalance。在SDK设计中，我们监听sarama-cluster发送的Rebalance Notification，根据此来：  
1. 销毁已创建的partition consumer worker（woker即对应每一个partition consumer的处理协程）；  
2. 如果Rebalance成功，则启动worker，开始消费message；  
3. 如果Rebalance失败，当前版本是主动close内的partition consumer，此举会出发新一次Rebalance。  
    但是此操作没有测试过，不知是否能成功。还有另外一个方式就是主动close当前Consumer，可以通过监控看到此机器上的consumer没有消费了，通过监控以达到通知的目的。

## TO-DO

1. 为Kafka SDK增加单独的日志，这样可以很好监控。当前是使用日志的关键字`Kafka Producer`和`Kafka Consumer`；  
2. 测试Consumer里Rebalance Error的情况;  
3. 经测试，发现在kafka-manager里动态增加partition，producer能反应，马上发送message到新的partition，而consumer group却没有。都是设置了`metadata.fresh.frequency`，不知道为何consumer group没有起作用，需要查看下是哪里问题。  



