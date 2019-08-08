## Server-Client
Client and server can be initiated individually in Cogman. And any number of cogman client and server can be run in different machine to pull and push task from same source. 


#### Client Example
For client connection we need a new session. From there we can connect and initiate the queue. It will start re-enqueue and reconnection process.

##### config 
```go
cfg := config.Client{
    ConnectionTimeout: time.Minute * 10, // default .server will stop trying to reconnection if it is not done within connection timeout
    RequestTimeout:    time.Second * 10, // default value 5 second. How long client will wait for delivery ack.

    Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017/", TTL: time.Hour}, // default
    Redis: config.Redis{URI: "redis://localhost:6379/0", TTL: time.Hour},               // required
    AMQP: config.AMQP{
        URI:                    "amqp://localhost:5672",
        HighPriorityQueueCount: 5,  // default value 1
        LowPriorityQueueCount:  5,  // default value 1
        Exchange:               "", // Client and Server exchange should be same
    },

    ReEnqueue: true, // default value false. Mongo connection required if ReEnqueue: true
}
```
Cogman client follow round robin process to decide in which queue task will be push. 

```go
clnt, err := cogman.NewSession(cfg)
if err != nil {
    log.Fatal(err)
}

if err := clnt.Connect(); err != nil {
    log.Fatal(err)
}
```

For sending task, cogman client has individual method. Before task send, a handler must be registered for the task. 
```go
if err := clnt.SendTask(task); err != nil {
    return err
}
```

#### Server Example
##### config:
```go
cfg := config.Server{
    ConnectionTimeout: time.Minute * 10, // default .server will stop trying to reconnection if it is not done within connection timeout

    Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017/", TTL: time.Hour}, // default
    Redis: config.Redis{URI: "redis://localhost:6379/0", TTL: time.Hour},               // required
    AMQP: config.AMQP{
        URI:                    "amqp://localhost:5672",
        HighPriorityQueueCount: 5,  // default
        LowPriorityQueueCount:  5,  // default
        Exchange:               "", // Client and Server exchange should be same
    },
}
```

Cogman server can pull task from any number of queue and process then asynchronously.
```go
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

And also server must register the task handler before start processing them.

```go
srvr.Register(exampletasks.TaskAddition, exampletasks.NewSumTask())
srvr.Register(exampletasks.TaskSubtraction, exampletasks.NewSubTask())
srvr.Register(exampletasks.TaskMultiplication, exampletasks.NewSubTask())
```