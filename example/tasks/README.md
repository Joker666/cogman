### Task

```go
type Task struct {
	TaskID         string       // unique .ID should be assigned by Cogman.
	Name           string       // required. And Task name must be registered with a task handler
	OriginalTaskID string       // a retry task will carry it's parents ID.
	Retry          int          // default value 0.
	Payload        []byte       // required
	Priority       TaskPriority // required. High or Low
	Status         Status       // current task status
	FailError      string       // default empty. If Status is failed, it must have a value.
	Duration       *float64     // task execution time.
	CreatedAt      time.Time    // create time.
	UpdatedAt      time.Time    // last update time.
}
```

---
### Handler Interface

```go
type Handler interface {
	Do(ctx context.Context, payload []byte) error
}
```



#### Handler Struct

A struct need to implement handler interface:

##### example

```go
type SumTask struct {
	Name string 
}

func (t SumTask) Do(ctx context.Context, payload []byte) error {
	var body TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Print("Sum task process error", err)
		return err
	}

	log.Printf("num1: %d num2: %d sum: %d", body.Num1, body.Num2, body.Num1+body.Num2)
	return nil
}

handler := exampletasks.NewSumTask()
if err := cogman.SendTask(*task, handler); err != nil {
	log.Fatal(err)
}
```

#### HandlerFunc 

A function type `HandlerFunc` also can passed as handler. 

```go
type HandlerFunc func(ctx context.Context, payload []byte) error

func (h HandlerFunc) Do(ctx context.Context, payload []byte) error {
	return h(ctx, payload)
}
``` 

##### example
```go
handlerFunc := util.HandlerFunc(func(ctx context.Context, payload []byte) error {
	log.Printf("Task process by handlerfunc")

	var body exampletasks.TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Print("Sub task process error", err)
		return err
	}

	log.Printf("num1: %d num2: %d sub: %d", body.Num1, body.Num2, body.Num1-body.Num2)
	return nil
})

if err := cogman.SendTask(*task, handlerFunc); err != nil {
	log.Fatal(err)
}
```