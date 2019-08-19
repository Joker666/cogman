### Simple example
---

Congman provide individual api and config method to make it easier to use. Just calling the api will launch client and server together. Here registering task handler & sending task can be done on fly.  

* [Config setup](#config)
* [Start Background API](#start-background)
* [Send Task API](#send-task)
* [Register API](#register)

#### Config:

Cogman provide most simplest approach to setup config file. It only ask for `amqp` & `redis` connection address.

Here `mongo` is optional. But for re-enqueuing task, It's required. Other field will be fill up by default value if they are empty.

```go
cfg := &config.Config{
	AmqpURI:  "amqp://localhost:5672",    // required
	RedisURI: "redis://localhost:6379/0", // required
}
```

#### Start Background
this simple api call will start a client and a server. 

```go
if err := cogman.StartBackground(cfg); err != nil {
    log.Fatal(err)
}
```

#### Send Task
Send task will deliver the task to `amqp`. Task also need a handler to execute that task. Handler can be register individually using [Register](#register) api. Then handler parameter in `SendTask` should be `nil`.

```go
if err := cogman.SendTask(task, handler); err != nil {
    log.Fatal(err)
}
```

#### Register
For registering a task handler at any point of the program, register api can be used. `handler` is an interface that only required a `Do` method to implement.

```go
cogman.Register(exampletasks.TaskAddition, handler)
if err != nil {
    log.Fatal(err)
}
```