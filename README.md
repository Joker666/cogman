[![CircleCI](https://circleci.com/gh/Joker666/cogman.svg?style=svg)](https://circleci.com/gh/Joker666/cogman) [![Go Report Card](https://goreportcard.com/badge/Joker666/cogman)](https://goreportcard.com/report/github.com/Joker666/cogman) [![GitHub](https://img.shields.io/github/license/Joker666/cogman)](https://github.com/Joker666/cogman/blob/master/LICENSE) [![godoc for Joker666/cogman](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/Joker666/cogman)

![logo](https://i.imgur.com/by0sIiG.png)

## Table of Contents
- [How to Use](#how-to-use)
- [Motivation](#motivation)
- [Requirements](#requirements)
- [Features](#features)
- [Examples](#examples)
- [Setup](#setup)
    - [Config](#config)
    - [Client/Server](#clientserver)
    - [Task](#ask)
    - [Worker/Task Hander](#workertask-handler)
    - [Register The Handlers](#register-the-handlers)
    - [Send task](#send-task)
- [Feature Comparison](#feature-comparison)
- [Contribution](#contribution)
- [License](#license)

## How to use: 
First add it to `$GOPATH`

```
go get github.com/Joker666/cogman
```

Then add [configuration](#Config) for rabbitmq for messaging, redis as backend, optionally mongodb as backend for re-enqueuing feature. <br />
Start the server to consume the tasks and start the client session to send tasks to server. [Start server and client](#clientserver).  <br />
Write [task handlers](#workertask-handler) and [register](#register-the-handlers) them.
[Send the tasks](#send-task) to process.
And voila!, you have setup the simplest background processing job server.

You should see something like this when everything is up and running
![List of services](https://i.imgur.com/AyIJkW8.png)

## Motivation
In python world you have [Celery](https://github.com/celery/celery), in Ruby world you have [Resque](https://github.com/resque/resque), [SideKiq](https://github.com/mperham/sidekiq), in C# [Hangfire](https://github.com/HangfireIO/Hangfire). All of them had one thing in common, simple interface to get started. When building products in Golang, this was apparent that, there is no library with a simple interface. We have Machinery, which is an excellent library, but has a steep learning curve. Also it has tonnes of features backed in that you do not need and but is required anyway.

Also the way it handled processing of future tasks with RabbitMQ's [Dead Letter Exchange](https://www.rabbitmq.com/dlx.html), we were not very fond of it. So we decided to make our own job processing library. This is a opinionated library as it uses [RabbitMQ](https://www.rabbitmq.com/) as the message broker and [Redis](https://redis.io/) and optionally [MongoDB](https://www.mongodb.com/) backend for more features. This setup has worked great for us in production and under stress and I believe this can work for large tasks as well

## Requirements
- Go
- RabbitMQ  
- Redis  
- MongoDB (optional)

## Features

- [x] Task Priority
- [x] Persistence
- [x] [Queue](#queue) type
- [x] Retries
- [x] Multiple consumer & server
- [x] Concurrency
- [x] Redis and Mongo log
- [x] [Re-enqueue](#re-enqueue) recovered task
- [x] Handle [Reconnection](#re-connection)
- [ ] UI
- [x] Rest API

## Examples
- [Simple use](https://github.com/Joker666/cogman/tree/master/example/simple)
- [Queue type](https://github.com/Joker666/cogman/tree/master/example/queue)
- [Client & Server](https://github.com/Joker666/cogman/tree/master/example/client-server)
- [Task & Task Handler](https://github.com/Joker666/cogman/tree/master/example/tasks)

## Setup
### Config 

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

### Client/Server

This Cogman api call will start a client and a server.

```go
if err := cogman.StartBackground(cfg); err != nil {
    log.Fatal(err)
}
```

Instead, if you want you can initiate Client & Server individually:

```go
// Client
client, err := cogman.NewSession(cfg)
if err != nil {
    log.Fatal(err)
}

if err := client.Connect(); err != nil {
    log.Fatal(err)
}

// Server
server, err := cogman.NewServer(cfg)
if err != nil {
    log.Fatal(err)
}

go func() {
    defer server.Stop()
    if err = server.Start(); err != nil {
        log.Fatal(err)
    }
}()

```

### Task
Tasks are grouped by two priority level. Based on that it will be assigned to a queue.

```go
type Task struct {
    TaskID         string       // unique. ID should be assigned by Cogman.
    Name           string       // required. And Task name must be registered with a task handler
    OriginalTaskID string       // a retry task will carry it's parents ID.
    PrimaryKey     string       // optional. Client can set any key to trace a task.
    Retry          int          // default value 0.
    Prefetch       int          // optional. Number of task fetch from queue by consumer at a time.
    Payload        []byte       // required
    Priority       TaskPriority // required. High or Low
    Status         Status       // current task status
    FailError      string       // default empty. If Status is failed, it must have a value.
    Duration       *float64     // task execution time.
    CreatedAt      time.Time    // create time.
    UpdatedAt      time.Time    // last update time.
}
```

### Worker/Task Handler

Any struct can be passed as a handler it implements below `interface`:

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

### Register The Handlers
```go
// Register task handler from Server side
server.Register(taskName, handler)
server.Register(taskName, handlerFunc)
```

### Send Task

Sending task using Cogman API:
```go
if err := cogman.SendTask(*task, handler); err != nil {
	log.Fatal(err)
}

// If a task handler is already registered, you can pass nil.
if err := cogman.SendTask(*task, nil); err != nil {
	log.Fatal(err)
}

```

Sending task using Cogman Client/Server:

```go
// Sending task from client
if err := client.SendTask(task); err != nil {
    return err
}
```

### Queue

Cogman queue type:

```
- High_Priority_Queue [default Queue]  
- Low_Priority_Queue  [lazy Queue]
```

There are two types queues that Cogman maintains. Default & Lazy queue.
High priority tasks would be pushed to default queue and low priority task would be pushed to lazy queue.
The number of each type of queues can be set by client/server through configuration.
Queue won't be lost after any sort of connection interruption.

### Re-Connection

Cogman Client & Server both handles reconnection. If the client loses connection, it can still take tasks,
and those will be processed immediate after Cogman client gets back the connection.
After Server reconnects, it will start to consume tasks without losing any task.

### Re-Enqueue

Re-enqueue feature to recover all the initiated task those are lost for connection error.
If client somehow loses the amqp connection, Cogman can still take the task in offline.
All offline task will be re-queue after connection re-established.
Cogman fetches all the offline tasks from mongo logs, and re-initiate them. Mongo connection required here.
For re-enqueuing, task retry count would not change.


## Feature Comparison

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
| Persistence              |      ✓      | ✓             |
| UI                       |             |               |
| Rest API                 |      ✓      |               |
| Chain                    |             | ✓             |
| Chords                   |             | ✓             |
| Groups                   |             | ✓             |


## Contribution
Want to contribute? Great!

To fix a bug or enhance an existing module, follow these steps:

- Fork the repo
- Create a new branch (`git checkout -b improve-feature`)
- Make the appropriate changes in the files
- Add changes to reflect the changes made
- Commit your changes (`git commit -am 'Improve feature'`)
- Push to the branch (`git push origin improve-feature`)
- Create a Pull Request 

### Bug / Feature Request
If you find a bug, kindly open an issue [here](https://github.com/Joker666/cogman/issues/new).<br/>
If you'd like to request/add a new function, feel free to do so by opening an issue [here](https://github.com/Joker666/cogman/issues/new). 

## [License](https://github.com/Joker666/cogman/blob/master/LICENSE.md)

MIT © [MD Ahad Hasan](https://github.com/joker666)