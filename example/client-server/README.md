## Server-Client
Client and server can be initiated individually in Cogman.

#### Client Example
Create a new session for making a new client connection.
This also initiates the queues.
`client.connect` also initiates a re-enqueue process if it gets it from the config.

Cogman stores task logs if somehow `amqp` loses the connection so that they can be re-processed.
When the client re-starts, it fetches all the tasks from `mongodb` with status `initiated` and processes them again.

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
Cogman client follows round robin approach to decide in which queue tasks will be pushed within the priority. 

```go
client, err := cogman.NewSession(cfg)
if err != nil {
    log.Fatal(err)
}

if err := client.Connect(); err != nil {
    log.Fatal(err)
}
```

Cogman client has a simple API for sending tasks to server. Each task must have a registered task handler. 
```go
if err := client.SendTask(task); err != nil {
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
        HighPriorityQueueCount: 5,  // optional. default value 1
        LowPriorityQueueCount:  5,  // optional. default value 1
        Exchange:               "", // Client and Server exchange should be same
    },
    StartRestServer: true // optional. default value false
}
```

Cogman server can pull task from any number of queues and processes them asynchronously.
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
srvr.Register(exampleTasks.TaskAddition, exampletasks.NewSumTask())
srvr.Register(exampleTasks.TaskSubtraction, exampletasks.NewSubTask())
srvr.Register(exampleTasks.TaskMultiplication, exampletasks.NewSubTask())
```