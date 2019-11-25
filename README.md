<h1 align="center">  Cogman </h1>

A distributed task runner.

---

### Clone & Build: 
```
go get github.com/Tapfury/cogman
cd cogman
./build.sh
```

### Requirements
- Go
- RabbitMQ  
- Redis  
- MongoDB (optional)

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
- [ ] UI
- [x] Rest API

### Example
---

#### Config 

Cogman api config example.
```go
cfg := &config.Config{
    ConnectionTimeout: time.Minute * 10, // default value 10 minutes
    RequestTimeout   : time.Second * 5,  // default value 5 second


    AmqpURI : "amqp://localhost:5672",                  // required
    RedisURI: "redis://localhost:6379/0",               // required
    MongoURI: "mongodb://root:secret@localhost:27017/", // optional

    RedisTTL: time.Hour * 24 * 7,  // default value 1 week
    MongoTTL: time.Hour * 24 * 30, // default value 1 month

    HighPriorityQueueCount: 2,     // default value 1
    LowPriorityQueueCount : 4,     // default value 1
}
```

Client & Server have individual config file to use them separately.

#### Client/Server

This Congman api call will start a client and a server.

```go
if err := cogman.StartBackground(cfg); err != nil {
    log.Fatal(err)
}
```

Initiate Client & Server individually:

```go
// Client
clnt, err := cogman.NewSession(cfg)
if err != nil {
    log.Fatal(err)
}

if err := clnt.Connect(); err != nil {
    log.Fatal(err)
}

// Server
srvr, err := cogman.NewServer(cfg)
if err != nil {
    log.Fatal(err)
}

go func() {
    defer srvr.Stop()
    if err = srvr.Start(); err != nil {
        log.Fatal(err)
    }
}()

```

#### Task
Tasks are grouped by two priority level. Based on that it will be assigned to a queue.

```go
type Task struct {
    TaskID         string       // unique. ID should be assigned by Cogman.
    Name           string       // required. And Task name must be registered with a task handler
    OriginalTaskID string       // a retry task will carry it's parents ID.
    PrimaryKey     string       // optional. Client can set any key to trace a task.
    Retry          int          // default value 0.
    Prefetch      int          // optional. Number of task fetch from queue by consumer at a time.
    Payload        []byte       // required
    Priority       TaskPriority // required. High or Low
    Status         Status       // current task status
    FailError      string       // default empty. If Status is failed, it must have a value.
    Duration       *float64     // task execution time.
    CreatedAt      time.Time    // create time.
    UpdatedAt      time.Time    // last update time.
}
```

#### Worker/Task Handler

Any struct can be passed as a handler it implement below `interface`:

```go
type Handler interface {
	Do(ctx context.Context, payload []byte) error
}
``` 

A function type `HandlerFunc` also can pass as handler.

```go
type HandlerFunc func(ctx context.Context, payload []byte) error

func (h HandlerFunc) Do(ctx context.Context, payload []byte) error {
	return h(ctx, payload)
}
```

#### Send Task

Sending task using Cogman API:
```go
if err := cogman.SendTask(*task, handler); err != nil {
	log.Fatal(err)
}

// If a task handler already registered for the task, task handler wont required further.
if err := cogman.SendTask(*task, nil); err != nil {
	log.Fatal(err)
}

``` 

Sending task using Cogman Client/Server:

```go
// Register task handler from Server side
server.Register(taskName, handler)
server.Register(taskName, handlerFunc)

// Sending task from client
if err := client.SendTask(task); err != nil {
    return err
}
```

#### Queue

Cogman queue type:

```
- High_Priority_Queue [default Queue]  
- Low_Priority_Queue  [lazy Queue]
```

Two type queue Cogman maintain. Default & Lazy queue. All the high priority task will be push to default queue and low priority task will be push to lazy queue. The number of each type of queue can be set by client/server. And queue won't be lost after any sort of connection interruption.

#### Re-Connection

Cogman Client & Server both handler reconnection. If Client loss connection, it can still take task and those will be processed immediate after Cogman client get back the connection.
After Server reconnect, it will start to consume task without losing any task.

#### Re-Enqueue

Re-enqueue feature to recover all the initiated task those are lost for connection error. If client somehow lost the amqp connection, Cogman can still take the task in offline. All offline task will be re-queue after connection re-established. Cogman fetch all the offline task from mongo logs, and re-initiate them. Mongo connection required here. For re-enqueuing, task retry count wont be changed.

#### Implementation
- [Simple use](https://github.com/Tapfury/cogman/tree/master/example/simple)
- [Queue type](https://github.com/Tapfury/cogman/tree/master/example/queue)
- [Client & Server](https://github.com/Tapfury/cogman/tree/master/example/client-server)
- [Task & Task Handler](https://github.com/Tapfury/cogman/tree/master/example/tasks)


### Feature Comparison

Comparison among the other job/task process runner.

| Feature                  | Cogman      | Machinery     |
| :------------------------|:-----------:|:-------------:|
| Backend                  | redis/mongo | redis         |
| Priorities               |      ✓      | ✓             |
| Re-Enqueue               |      ✓      |               |
| Concurrency              |      ✓      | ✓             |
| Re-Connection            |      ✓      |               |
| Delayed jobs             |             | ✓             |
| Concurrent client/server |      ✓      |               |
| Re-try                   |      ✓      | ✓             |
| Persistence              |      ✓      |               |
| UI                       |             |               |
| Rest API                 |      ✓      |               |
| Chain                    |             | ✓             |
| Chords                   |             | ✓             |
| Groups                   |             | ✓             |  
 