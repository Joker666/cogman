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
	Mcl *mongo.Client
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
		Mcl: conn,
	}, nil
}

func (s *MongoClient) Ping() error {
	return s.Mcl.Ping(context.Background(), readpref.Primary())
}

func (s *MongoClient) Close() error {
	return s.Mcl.Disconnect(context.Background())
}

func (s *MongoClient) Connect() error {
	err := s.Mcl.Connect(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (s *MongoClient) getCollection() (*mongo.Collection, error) {
	return s.Mcl.Database(database).Collection(tableTasks), nil
}

type bsonTask struct {
	ID             primitive.ObjectID `bson:"_id"`
	TaskID         string             `bson:"task_id"`
	Name           string             `bson:"name"`
	Payload        []byte             `bson:"payload"`
	Status         string             `bson:"status"`
	PreviousTaskID string             `bson:"previous_task_id"`
	FailError      string             `bson:"fail_error"`
	CreatedAt      time.Time          `bson:"created_at"`
	UpdatedAt      time.Time          `bson:"updated_at"`
}

func prepareBsonTask(t *util.Task) *bsonTask {
	return &bsonTask{
		TaskID:  t.ID,
		Name:    t.Name,
		Payload: t.Payload,
		// "previous_task_id": t.PreviousTaskID
		Status:    string(t.Status),
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
	p := bson.M{
		"task_id": id,
	}

	if err := col.FindOne(ctx, p).Decode(task); err != nil {
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

	p := bson.M{
		"_id": task.ID,
	}

	_, err = col.ReplaceOne(ctx, p, task)
	if err != nil {
		return err
	}

	return nil
}
