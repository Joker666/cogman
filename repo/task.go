package repo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Tapfury/cogman/infra"
	"github.com/Tapfury/cogman/util"

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

func (s *TaskRepository) Indices() []infra.Index {
	return []infra.Index{
		{
			Name: "task_id_type",
			Keys: []infra.IndexKey{
				{"task_id", false},
			},
			Unique: true,
			Sparse: false,
		},
		{
			Name: "primary_key_type",
			Keys: []infra.IndexKey{
				{"task_id", false},
				{"primary_key", false},
			},
			Unique: true,
			Sparse: false,
		},
		{
			Name: "task_list_type",
			Keys: []infra.IndexKey{
				{"status", false},
				{"created_at", true},
			},
			Unique: false,
			Sparse: false,
		},
		{
			Name: "task_filter_type",
			Keys: []infra.IndexKey{
				{"task_id", false},
				{"primary_key", false},
				{"status", false},
				{"created_at", true},
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

// DropIndices drop all the listed indice
func (s *TaskRepository) DropIndices() error {
	return s.MongoConn.DropIndices()
}

// CreateTask create a task object both in redis and mongo
func (s *TaskRepository) CreateTask(task *util.Task, createdAt *time.Time) error {
	if task.Status != util.StatusRetry {
		task.Status = util.StatusInitiated
	}

	nw := time.Now()
	if createdAt == nil {
		createdAt = &nw
	}

	task.UpdatedAt = nw
	task.CreatedAt = *createdAt
	go func() {
		if s.MongoConn == nil {
			return
		}

		t := prepareBsonTask(task)
		if t.ID.IsZero() {
			t.ID = primitive.NewObjectID()
		}

		err := s.MongoConn.Create(t)
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

		byts, err := json.Marshal(task)
		if err != nil {
			errs = err
			return
		}

		err = s.RedisConn.Create(task.TaskID, byts)
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

	byts, err := s.RedisConn.Get(id)
	if err != nil {
		return nil, err
	}
	if byts == nil {
		return nil, ErrTaskNotFound
	}

	task := &util.Task{}
	if err := json.Unmarshal(byts, &task); err != nil {
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
func (s *TaskRepository) UpdateTaskStatus(id string, status util.Status, args ...interface{}) {
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
			s.lgr.Error("UpdateTaskStatus", ErrDurationRequired)
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

		numA, numB := int64(0), int64(1)

		for i := 0; i < 6; i++ {
			time.Sleep(time.Second * time.Duration(numB))
			numB, numA = nextFibonacciNumber(numA, numB), numB

			if s.MongoConn == nil {
				return
			}

			errs = nil
			func() {
				resp, err := s.MongoConn.Get(q)
				if err != nil {
					errs = err
					return
				}

				if err := parseTask(resp, task); err != nil {
					errs = err
					return
				}

				if !status.CheckStatusOrder(util.Status(task.Status)) {
					return
				}

				task.Status = string(status)
				task.UpdatedAt = time.Now()
				task.Duration = duration
				task.FailError = ""
				if failError != nil {
					task.FailError = failError.Error()
				}

				if err = s.MongoConn.Update(q, task); err != nil {
					errs = err
				}
			}()

			if errs == nil {
				return
			}
		}

		s.lgr.Error("failed to update task", errs, util.Object{Key: "TaskID", Val: id}, util.Object{"Status", status})
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

		byts, err := json.Marshal(task)
		if err != nil {
			errs = err
			return
		}

		errs = s.RedisConn.Update(task.TaskID, byts)

	}()

	if errs != nil {
		s.lgr.Error("failed to update task", errs, util.Object{Key: "TaskID", Val: id}, util.Object{"Status", status})
	}
}

// UpdateRetryCount update task retry count field
func (s *TaskRepository) UpdateRetryCount(id string, count int) {
	go func() {
		var errs error

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

			errs = s.MongoConn.UpdatePartial(q, val)
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

		byts, err := json.Marshal(task)
		if err != nil {
			errs = err
			return
		}

		errs = s.RedisConn.Update(task.TaskID, byts)
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

	task := []*util.Task{}
	cursor, err := s.MongoConn.List(q, skip, limit)
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
	cursor, err := s.MongoConn.List(q, skip, limit)
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

	bndr := mgoTimeRangeBucketBoundaries(startTime, endTime, interval)
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
				"boundaries": bndr,
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

	cursor, err := s.MongoConn.Aggregate(q)
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
	bndr := []time.Time{}
	bndr = append(bndr, startDate)
	intlDt := startDate
	enDt := endDate
	for {
		intlDt = intlDt.Add(time.Minute * time.Duration(interval))
		if intlDt.After(endDate) || intlDt.Equal(endDate) {
			break
		}
		bndr = append(bndr, intlDt)
	}

	return append(bndr, enDt.AddDate(0, 0, 1))
}
