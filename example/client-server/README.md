## Server-Client
Client and server can be initiated individually in Cogman. And any number of cogman client and server can be run in different machine to pull and push task from same source. 


#### Client Example
For client connection we need a new session. From there we can connect and initiate the queue. It will start re-enqueue and reconnection process. 

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