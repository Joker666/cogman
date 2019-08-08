# Cogman

A distributed task runner.

---

### Clone & Build: 
```
go get github.com/Tapfury/cogman
cd cogman
./build
```


### Example:
- [Simple use](https://github.com/Tapfury/cogman/tree/example/example/simple)
- [Queue type](https://github.com/Tapfury/cogman/tree/example/example/queue)
- [Client & Server](https://github.com/Tapfury/cogman/tree/example/example/client-server)
- [Task & Task Handler](https://github.com/Tapfury/cogman/tree/example/example/tasks)


### Features
- [x] Task Priority
- [x] Persistence
- [x] [Queue](#queue) type
- [x] Retries
- [x] Multiple consumer & server
- [x] Concurrency
- [x] Redis and Mongo log
- [x] [Re-enqueue](#re-enqueue) recovered task
- [x] Handle reconnection

##### Queue

Two type queue Cogman maintain. Default & Lazy queue. All the high priority task will be push to default queue and low priority task will be push to lazy queue. The number of each type of queue can be set by client/server. And queue won't be lost after any sort of connection interruption.

##### Re-Enqueue

Re-enqueue feature to recover all the initiated those are lost for connection error. If client some how lost the amqp connection, Cogman can still take the task in offline. All offline task will be re-queue after connection re-established.  Cogman fetch all the offline task from mongo logs, and re-initiate them. Mongo connection required here. For re-enqueuing, task retry count wont be changed.


### Feature Comparison

Comparison among the other job/task process runner.

| Feature                  | Cogman      | Bull          | Kue   | Bee      | Agenda |
| :------------------------|:-----------:|:-------------:|:-----:|:--------:|:------:|
| Backend                  | redis/mongo | redis         | redis |   redis  | mongo  |
| Priorities               |      ✓      | ✓             |  ✓    |          |   ✓    |
| Concurrency              |      ✓      | ✓             |  ✓    |  ✓       |   ✓    |
| Delayed jobs             |             | ✓             |  ✓    |          |   ✓    |
| Multiple client & server |      ✓      | ✓             |  ✓    |          |        |
| Global events            |             | ✓             |  ✓    |          |        |
| Rate Limiter             |             | ✓             |       |          |        |
| Pause/Resume             |             | ✓             |  ✓    |          |        |
| Sandboxed worker         |             | ✓             |       |          |        |
| Repeatable jobs          |             | ✓             |       |          |   ✓    |
| Atomic ops               |             | ✓             |       |    ✓     |        |
| Persistence              |      ✓      | ✓             |   ✓   |    ✓     |   ✓    |
| UI                       |             | ✓             |   ✓   |          |   ✓    |
| Optimized for            |             |Jobs / Messages| Jobs  | Messages |  Jobs  |
