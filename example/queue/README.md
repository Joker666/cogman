#### Task Priority
---
Cogman sorted the task into two priority. 
```
- TaskPriorityHigh  
- TaskPriorityLow  
``` 

#### Queue configuration
---

Cogman can process low and high priority task separately. It maintain two types of queue: `High Priority Queue` & `Low Priority Queue`. Based on task priority, they will be push to one of this queue. The queue number can be configured. 
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

> **Note:** In default case, there will be at least one high Priority queue and one low priority queue. 

