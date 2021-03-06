package repo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Joker666/cogman/infra"
	"github.com/Joker666/cogman/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// TaskRepository contain necessary fields
type TaskRepository struct {
	RedisConn *infra.RedisClient
	MongoConn *infra.MongoClient

	lgr util.Logger
}

// NewTaskRepo return a new TaskRepository instance
func NewTaskRepo(redisCon *infra.RedisClient, mgoCon *infra.MongoClient) *TaskRepository {
	return &TaskRepository{
		RedisConn: redisCon,
		MongoConn: mgoCon,

		lgr: util.NewLogger(),
	}
}

// CloseClients close connection
func (s *TaskRepository) CloseClients() {
	_ = s.RedisConn.Close()
	_ = s.MongoConn.Close()
}

type bsonTask struct {
	ID             primitive.ObjectID `bson:"_id"`
	TaskID         string             `bson:"task_id"`
	PrimaryKey     string             `bson:"primary_key"`
	Name           string             `bson:"name"`
	OriginalTaskID string             `bson:"original_task_id"`
	Payload        []byte             `bson:"payload"`
	Priority       string             `bson:"priority"`
	Status         string             `bson:"status"`
	Retry          int                `bson:"retry"`
	FailError      string             `bson:"fail_error"`
	Duration       *float64           `bson:"duration"`
	CreatedAt      time.Time          `bson:"created_at"`
	UpdatedAt      time.Time          `bson:"updated_at"`
}

func prepareBsonTask(t *util.Task) *bsonTask {
	return &bsonTask{
		TaskID:         t.TaskID,
		PrimaryKey:     t.PrimaryKey,
		Name:           t.Name,
		Payload:        t.Payload,
		Priority:       string(t.Priority),
		OriginalTaskID: t.OriginalTaskID,
		Retry:          t.Retry,
		Status:         string(t.Status),
		FailError:      t.FailError,
		Duration:       t.Duration,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
	}
}

func formTask(t *bsonTask) *util.Task {
	return &util.Task{
		TaskID:         t.TaskID,
		Name:           t.Name,
		PrimaryKey:     t.PrimaryKey,
		Payload:        t.Payload,
		Priority:       util.TaskPriority(t.Priority),
		OriginalTaskID: t.OriginalTaskID,
		Retry:          t.Retry,
		Status:         util.Status(t.Status),
		FailError:      t.FailError,
		Duration:       t.Duration,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
	}
}

// Indices are indices
func (s *TaskRepository) Indices() []infra.Index {
	return []infra.Index{
		{
			Name: "task_id_type",
			Keys: []infra.IndexKey{
				{Key: "task_id", Desc: false},
			},
			Unique: true,
			Sparse: false,
		},
		{
			Name: "primary_key_type",
			Keys: []infra.IndexKey{
				{Key: "task_id", Desc: false},
				{Key: "primary_key", Desc: false},
			},
			Unique: true,
			Sparse: false,
		},
		{
			Name: "task_list_type",
			Keys: []infra.IndexKey{
				{Key: "status", Desc: false},
				{Key: "created_at", Desc: true},
			},
			Unique: false,
			Sparse: false,
		},
		{
			Name: "task_filter_type",
			Keys: []infra.IndexKey{
				{Key: "task_id", Desc: false},
				{Key: "primary_key", Desc: false},
				{Key: "status", Desc: false},
				{Key: "created_at", Desc: true},
			},
			Unique: false,
			Sparse: false,
		},
	}
}

// EnsureIndices ensure listed indices
func (s *TaskRepository) EnsureIndices() error {
	return s.MongoConn.EnsureIndices(s.Indices())
}

// DropIndices drop all the listed indices
func (s *TaskRepository) DropIndices() error {
	return s.MongoConn.DropIndices()
}

// CreateTask create a task object both in redis and mongo
func (s *TaskRepository) CreateTask(task *util.Task) error {
	if task.Status != util.StatusRetry {
		task.Status = util.StatusInitiated
	}

	nw := time.Now()
	task.UpdatedAt = nw
	task.CreatedAt = nw

	go func() {
		if s.MongoConn == nil {
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		t := prepareBsonTask(task)
		if t.ID.IsZero() {
			t.ID = primitive.NewObjectID()
		}

		err := s.MongoConn.Create(ctx, t)
		if err != nil {
			s.lgr.Error("failed to create task", err)
		}
	}()

	var errs error

	func() {
		if s.RedisConn == nil {
			errs = ErrRedisNoConnection
			return
		}

		bytes, err := json.Marshal(task)
		if err != nil {
			errs = err
			return
		}

		err = s.RedisConn.Create(task.TaskID, bytes)
		if err != nil {
			errs = err
			return
		}
	}()

	return errs
}

// GetTask return a id from redis
func (s *TaskRepository) GetTask(id string) (*util.Task, error) {
	if s.RedisConn == nil {
		return nil, ErrRedisNoConnection
	}

	bytes, err := s.RedisConn.Get(id)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, ErrTaskNotFound
	}

	task := &util.Task{}
	if err := json.Unmarshal(bytes, &task); err != nil {
		return nil, err
	}
	return task, nil
}

func nextFibonacciNumber(numberA, numberB int64) int64 {
	return numberA + numberB
}

func parseTask(resp *mongo.SingleResult, task *bsonTask) error {
	if err := resp.Decode(task); err != nil {
		return err
	}
	if task == nil {
		return ErrTaskNotFound
	}

	return nil
}

// UpdateTaskStatus update the task status
func (s *TaskRepository) UpdateTaskStatus(ctx context.Context, id string, status util.Status, args ...interface{}) {
	var failError error
	var duration *float64

	switch status {
	case util.StatusFailed:
		err, ok := args[0].(error)
		if !ok {
			s.lgr.Error("UpdateTaskStatus", ErrErrorRequired)
			return
		}
		failError = err
	case util.StatusSuccess:
		dur, ok := args[0].(float64)
		if !ok {
			s.lgr.Error("bytes", ErrDurationRequired)
			return
		}
		duration = &dur
	}
	go func() {
		var errs error
		q := bson.M{
			"task_id": id,
		}
		task := &bsonTask{}

		if s.MongoConn == nil {
			return
		}
		errs = nil
		func() {
			var err error
			trxID := util.GenerateRandStr(10)
			ctx, err = s.MongoConn.StartTransaction(ctx, trxID)
			if err != nil {
				errs = err
				return
			}

			if !(status == util.StatusSuccess || status == util.StatusFailed) {
				return
			}

			resp, err := s.MongoConn.Get(ctx, q)
			if err != nil {
				errs = err
				_, _ = s.MongoConn.AbortTransaction(ctx)
				return
			}

			if err := parseTask(resp, task); err != nil {
				errs = err
				_, _ = s.MongoConn.AbortTransaction(ctx)
				return
			}

			if !status.CheckStatusOrder(util.Status(task.Status)) ||
				(task.Status == string(util.StatusSuccess) || task.Status == string(util.StatusFailed)) {
				return
			}

			task.Status = string(status)
			task.UpdatedAt = time.Now()
			task.Duration = duration
			task.FailError = ""
			if failError != nil {
				task.FailError = failError.Error()
			}

			if err = s.MongoConn.Update(ctx, q, task); err != nil {
				_, _ = s.MongoConn.AbortTransaction(ctx)
				errs = err
			}
			_, err = s.MongoConn.CommitTransaction(ctx)
			if err != nil {
				errs = err
				return
			}
		}()
		if errs == nil {
			return
		}

		s.lgr.Error("warning to update task", errs, util.Object{Key: "TaskID", Val: id}, util.Object{Key: "Status", Val: status})
	}()

	var errs error

	func() {
		if s.RedisConn == nil {
			errs = ErrRedisNoConnection
			return
		}

		task, err := s.GetTask(id)
		if err != nil {
			errs = err
			return
		}

		if !status.CheckStatusOrder(task.Status) {
			return
		}

		task.Status = status
		task.UpdatedAt = time.Now()
		task.Duration = duration
		task.FailError = ""
		if failError != nil {
			task.FailError = failError.Error()
		}

		bytes, err := json.Marshal(task)
		if err != nil {
			errs = err
			return
		}

		errs = s.RedisConn.Update(task.TaskID, bytes)
	}()

	if errs != nil {
		s.lgr.Error("failed to update task", errs, util.Object{Key: "TaskID", Val: id}, util.Object{Key: "Status", Val: status})
	}
}

// UpdateRetryCount update task retry count field
func (s *TaskRepository) UpdateRetryCount(id string, count int) {
	go func() {
		var errs error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		q := bson.M{
			"task_id": id,
		}
		val := bson.M{
			"$set": bson.M{
				"updated_at": time.Now(),
			},
			"$inc": bson.M{
				"retry": count,
			},
		}

		numA, numB := int64(0), int64(1)

		for i := 0; i < 6; i++ {
			time.Sleep(time.Second * time.Duration(numB))
			numB, numA = nextFibonacciNumber(numA, numB), numB

			if s.MongoConn == nil {
				return
			}

			errs = s.MongoConn.UpdatePartial(ctx, q, val)
			if errs == nil {
				break
			}
		}

		if errs != nil {
			s.lgr.Error("failed to update retry count", errs, util.Object{Key: "TaskID", Val: id})
		}
	}()

	var errs error

	func() {
		if s.RedisConn == nil {
			errs = ErrRedisNoConnection
			return
		}

		task, err := s.GetTask(id)
		if err != nil {
			errs = err
			return
		}

		task.UpdatedAt = time.Now()
		task.Retry += count

		bytes, err := json.Marshal(task)
		if err != nil {
			errs = err
			return
		}

		errs = s.RedisConn.Update(task.TaskID, bytes)
	}()

	if errs != nil {
		s.lgr.Error("failed to update retry count", errs, util.Object{Key: "TaskID", Val: id})
	}
}

// ListByStatusBefore fetch a task list before time t
func (s *TaskRepository) ListByStatusBefore(status util.Status, t time.Time, skip, limit int) ([]*util.Task, error) {
	if s.MongoConn == nil {
		return nil, ErrMongoNoConnection
	}

	q := bson.M{
		"status": string(status),
		"created_at": bson.M{
			"$lte": t,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := []*util.Task{}
	cursor, err := s.MongoConn.List(ctx, q, skip, limit)
	if err != nil {
		return nil, err
	}

	for cursor.Next(context.Background()) {
		bTask := &bsonTask{}
		if err := cursor.Decode(bTask); err != nil {
			return nil, err
		}

		task = append(task, formTask(bTask))
	}

	return task, nil
}

// List send a task list
func (s *TaskRepository) List(v map[string]interface{}, startTime, endTime *time.Time, skip, limit int) ([]*util.Task, error) {
	if s.MongoConn == nil {
		return nil, ErrMongoNoConnection
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := bson.M{}
	for key, val := range v {
		q[key] = val
	}

	if !(startTime == nil || endTime == nil) {
		q["created_at"] = bson.M{
			"$gte": startTime,
			"$lte": endTime,
		}
	}

	task := []*util.Task{}
	cursor, err := s.MongoConn.List(ctx, q, skip, limit)
	if err != nil {
		return nil, err
	}

	for cursor.Next(context.Background()) {
		bTask := &bsonTask{}
		if err := cursor.Decode(bTask); err != nil {
			return nil, err
		}

		task = append(task, formTask(bTask))
	}

	return task, nil
}

type bsonTaskDateRangeCount struct {
	ID              time.Time `bson:"_id"`
	Total           int       `bson:"total"`
	CountRetry      int       `bson:"retry"`
	CountInitiated  int       `bson:"initiated"`
	CountQueued     int       `bson:"queued"`
	CountInProgress int       `bson:"in_progress"`
	CountSuccess    int       `bson:"success"`
	CountFailed     int       `bson:"failed"`
}

func formTaskDateRangeCount(t *bsonTaskDateRangeCount) *util.TaskDateRangeCount {
	return &util.TaskDateRangeCount{
		ID:              t.ID,
		Total:           t.Total,
		CountRetry:      t.CountRetry,
		CountInitiated:  t.CountInitiated,
		CountQueued:     t.CountQueued,
		CountInProgress: t.CountInProgress,
		CountFailed:     t.CountFailed,
		CountSuccess:    t.CountSuccess,
	}
}

// ListCountDateRangeInterval  send a bucket list
func (s *TaskRepository) ListCountDateRangeInterval(startTime, endTime time.Time, interval int) ([]util.TaskDateRangeCount, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bsonCond := func(status string) bson.M {
		return bson.M{
			"$sum": bson.M{
				"$cond": bson.M{
					"if": bson.M{
						"$eq": []interface{}{"$status", status},
					}, "then": 1, "else": 0,
				},
			},
		}
	}

	boundary := mgoTimeRangeBucketBoundaries(startTime, endTime, interval)
	q := []bson.M{
		bson.M{
			"$match": bson.M{
				"created_at": bson.M{
					"$gte": startTime,
					"$lte": endTime,
				},
			},
		},
		bson.M{
			"$bucket": bson.M{
				"groupBy":    "$created_at",
				"boundaries": boundary,
				"default":    "Other",
				"output": bson.M{
					"total": bson.M{
						"$sum": 1,
					},
					"retry":       bsonCond("retry"),
					"initiated":   bsonCond("initiated"),
					"queued":      bsonCond("queued"),
					"in_progress": bsonCond("in_progress"),
					"failed":      bsonCond("failed"),
					"success":     bsonCond("success"),
				},
			},
		},
	}

	cursor, err := s.MongoConn.Aggregate(ctx, q)
	if err != nil {
		return nil, err
	}

	task := []util.TaskDateRangeCount{}

	for cursor.Next(context.Background()) {
		bTask := &bsonTaskDateRangeCount{}
		if err := cursor.Decode(bTask); err != nil {
			return nil, err
		}

		task = append(task, *formTaskDateRangeCount(bTask))
	}

	return task, nil
}

func mgoTimeRangeBucketBoundaries(startDate, endDate time.Time, interval int) []time.Time {
	boundary := []time.Time{}
	boundary = append(boundary, startDate)
	intlDt := startDate
	enDt := endDate
	for {
		intlDt = intlDt.Add(time.Minute * time.Duration(interval))
		if intlDt.After(endDate) || intlDt.Equal(endDate) {
			break
		}
		boundary = append(boundary, intlDt)
	}

	return append(boundary, enDt.AddDate(0, 0, 1))
}
