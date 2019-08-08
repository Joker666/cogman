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

### Handler

A stuct need to implement this interface:
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