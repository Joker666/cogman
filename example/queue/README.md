#### Task Priority
---
Cogman sorts the tasks into two priorities. 
```
- TaskPriorityHigh  
- TaskPriorityLow  
```

#### Queue configuration
---

Cogman processes low and high priority task separately.
It maintains two types of queue: `High Priority Queue` & `Low Priority Queue`.
Based on task priority, it will be pushed to one of these queues.
The number of queues can be configured for better throughput.
```go
cfg := &config.Config{
    ConnectionTimeout: time.Minute * 10, // optional
    RequestTimeout:    time.Second * 5,  // optional

    AmqpURI:  "amqp://localhost:5672",                  // required
    RedisURI: "redis://localhost:6379/0",               // required
    MongoURI: "mongodb://root:secret@localhost:27017/", //optional

    HighPriorityQueueCount: 2, // Optional. Default value 1
    LowPriorityQueueCount:  1, // Optional. Default value 1

    ReEnqueue: true, // optional. default false. Mongo connection also needed
}
```

Cogman `hight_priority_queue` implemented over `amqp`'s default queue.
And `low_priority_queue` implemented over lazy queue.
Cogman Queues are persistent.

> **Note:** In default case, there will be at least one high Priority queue and one low priority queue. 

