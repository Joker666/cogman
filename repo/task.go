package repo

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Tapfury/cogman/infra"
	"github.com/Tapfury/cogman/util"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Task struct {
	RedisConn *infra.RedisClient
	MongoConn *infra.MongoClient
}

func (s *Task) CloseClients() {
	s.RedisConn.Close()
	s.MongoConn.Close()
}

type bsonTask struct {
	ID             primitive.ObjectID `bson:"_id"`
	TaskID         string             `bson:"task_id"`
	Name           string             `bson:"name"`
	Payload        []byte             `bson:"payload"`
	Priority       string             `bson:"priority"`
	Status         string             `bson:"status"`
	PreviousTaskID string             `bson:"previous_task_id"`
	FailError      string             `bson:"fail_error"`
	CreatedAt      time.Time          `bson:"created_at"`
	UpdatedAt      time.Time          `bson:"updated_at"`
}

func prepareBsonTask(t *util.Task) *bsonTask {
	return &bsonTask{
		TaskID:         t.ID,
		Name:           t.Name,
		Payload:        t.Payload,
		Priority:       string(t.Priority),
		PreviousTaskID: t.PreviousTaskID,
		Status:         string(t.Status),
		FailError:      t.FailError,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
	}
}

func formTask(t *bsonTask) *util.Task {
	return &util.Task{
		ID:             t.TaskID,
		Name:           t.Name,
		Payload:        t.Payload,
		Priority:       util.PriorityType(t.Priority),
		PreviousTaskID: t.PreviousTaskID,
		Status:         util.Status(t.Status),
		FailError:      t.FailError,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
	}
}

func (s *Task) CreateTask(task *util.Task) error {
	nw := time.Now()

	task.Status = util.StatusInitiated
	task.UpdatedAt = nw
	task.CreatedAt = nw

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
			log.Print("Mongo: failed to create task ", err)
		}
	}()

	var errs error

	func() {
		byts, err := json.Marshal(task)
		if err != nil {
			errs = err
			return
		}

		err = s.RedisConn.Create(task.ID, byts)
		if err != nil {
			errs = err
			return
		}
	}()

	return errs
}

func nextFibonacciNumber(numberA, numberB int64) int64 {
	return numberA + numberB
}

func (s *Task) UpdateTaskStatus(id string, status util.Status, failError error) {
	go func() {
		if s.MongoConn == nil {
			return
		}

		var errs error

		q := bson.M{
			"task_id": id,
		}
		task := &bsonTask{}

		numA, numB := int64(0), int64(1)

		for i := 0; i < 6; i++ {
			time.Sleep(time.Second * time.Duration(numB))
			numB, numA = nextFibonacciNumber(numA, numB), numB

			errs = nil
			func() {
				resp, err := s.MongoConn.Get(q)
				if err != nil {
					errs = err
					return
				}

				if err := resp.Decode(task); err != nil {
					errs = err
					return
				}

				if task == nil {
					errs = ErrTaskNotFound
					return
				}

				if !status.CheckStatusOrder(util.Status(task.Status)) {
					return
				}

				task.Status = string(status)
				task.UpdatedAt = time.Now()
				task.FailError = ""
				if failError != nil {
					task.FailError = failError.Error()
				}

				if errs = s.MongoConn.Update(q, task); err != nil {
					errs = err
				}
			}()

			if errs == nil {
				return
			}
		}
		log.Print("Mongo: failed to update task: ", id, " ", status, " ", errs)
	}()

	var errs error

	func() {
		byts, err := s.RedisConn.Get(id)
		if err != nil {
			errs = err
			return
		}

		task := util.Task{}
		if err := json.Unmarshal(byts, &task); err != nil {
			errs = err
			return
		}

		if !status.CheckStatusOrder(util.Status(task.Status)) {
			return
		}

		task.Status = status
		task.UpdatedAt = time.Now()
		task.FailError = ""
		if failError != nil {
			task.FailError = failError.Error()
		}

		byts, err = json.Marshal(task)
		if err != nil {
			errs = err
			return
		}

		errs = s.RedisConn.Update(task.ID, byts)

	}()

	if errs != nil {
		log.Print("Redis: failed to update task: ", id, " ", status, " ", errs)
	}
}

func (s *Task) ListByStatusBefore(status util.Status, t time.Time, skip, limit int) ([]*util.Task, error) {
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
