package infra

import (
	"context"
	"time"

	"github.com/Tapfury/cogman/util"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	database   = "cogman"
	tableTasks = "tasks"
)

type MongoClient struct {
	URL string
	mcl *mongo.Client
}

func NewMongoClient(url string) (*MongoClient, error) {
	conn, err := mongo.Connect(
		context.Background(),
		options.Client().ApplyURI(url),
	)

	if err != nil {
		return nil, err
	}

	return &MongoClient{
		URL: url,
		mcl: conn,
	}, nil
}

func (s *MongoClient) Ping() error {
	return s.mcl.Ping(context.Background(), readpref.Primary())
}

func (s *MongoClient) Close() error {
	return s.mcl.Disconnect(context.Background())
}

func (s *MongoClient) Connect() error {
	err := s.mcl.Connect(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (s *MongoClient) getCollection() (*mongo.Collection, error) {
	return s.mcl.Database(database).Collection(tableTasks), nil
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
		TaskID:   t.ID,
		Name:     t.Name,
		Payload:  t.Payload,
		Priority: string(t.Priority),
		// "previous_task_id": t.PreviousTaskID
		Status:    string(t.Status),
		FailError: t.FailError,
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
	}
}

func formTask(t *bsonTask) *util.Task {
	return &util.Task{
		ID:       t.TaskID,
		Name:     t.Name,
		Payload:  t.Payload,
		Priority: util.PriorityType(t.Priority),
		// "previous_task_id": t.PreviousTaskID
		Status:    util.Status(t.Status),
		FailError: t.FailError,
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
	}
}

func (s *MongoClient) getTask(id string) (*bsonTask, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return nil, err
	}

	task := &bsonTask{}
	q := bson.M{
		"task_id": id,
	}

	if err := col.FindOne(ctx, q).Decode(task); err != nil {
		return nil, err
	}

	return task, nil
}

func (s *MongoClient) CreateTask(t *util.Task) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return err
	}

	task := prepareBsonTask(t)

	if task.ID.IsZero() {
		task.ID = primitive.NewObjectID()
	}

	_, err = col.InsertOne(ctx, task)
	return err
}

func (s *MongoClient) UpdateTaskStatus(id string, status util.Status, failErr error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return err
	}

	task, err := s.getTask(id)
	if err != nil {
		return err
	}

	if !util.CheckStatusOrder(util.Status(task.Status), status) {
		return nil
	}

	task.Status = string(status)
	task.UpdatedAt = time.Now()
	task.FailError = ""
	if failErr != nil {
		task.FailError = failErr.Error()
	}

	q := bson.M{
		"task_id": task.TaskID,
	}

	_, err = col.ReplaceOne(ctx, q, task)
	if err != nil {
		return err
	}

	return nil
}

func (s *MongoClient) ListByStatus(status util.Status, skip, limit int) ([]*util.Task, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return nil, err
	}
	q := bson.M{
		"status": string(status),
	}

	task := []*util.Task{}

	opt := options.Find().SetSkip(int64(skip)).SetLimit(int64(limit))
	cursor, err := col.Find(ctx, q, opt)
	for cursor.Next(ctx) {
		bTask := &bsonTask{}
		if err := cursor.Decode(bTask); err != nil {
			return nil, err
		}

		task = append(task, formTask(bTask))
	}
	if err != nil {
		return nil, err
	}

	return task, nil
}
