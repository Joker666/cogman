## Server-Client
Client and server can be initiated individually in Cogman.

#### Client Example
Create a new session for making a new client connection. After that initiate the queues. `client.connect` also initiates a re-enqueue process if it gets it from config.

Cogman store task log if somehow `rabbitamq` loss the connection so that they can be re-processed. The moment client re-start, it fetch all the task from `mongo` with status `initiated` and process them again.

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
Cogman client follow round robin process to decide in which queue task will be pushed. 

```go
clnt, err := cogman.NewSession(cfg)
if err != nil {
    log.Fatal(err)
}

if err := clnt.Connect(); err != nil {
    log.Fatal(err)
}
```

Cogman client has individual API for sending task. Each task must a registered task handler. 
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

Server must register the task handler before start processing them.

```go
srvr.Register(exampletasks.TaskAddition, exampletasks.NewSumTask())
srvr.Register(exampletasks.TaskSubtraction, exampletasks.NewSubTask())
srvr.Register(exampletasks.TaskMultiplication, exampletasks.NewSubTask())
```