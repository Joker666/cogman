package infra

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	database   = "cogman"
	tableTasks = "tasks"
)

type MongoClient struct {
	URL    string
	ExpDur int32
	mcl    *mongo.Client
}

func NewMongoClient(url string, ttl time.Duration) (*MongoClient, error) {
	conn, err := mongo.Connect(
		context.Background(),
		options.Client().ApplyURI(url),
	)

	if err != nil {
		return nil, err
	}

	return &MongoClient{
		URL:    url,
		ExpDur: int32(ttl.Seconds()),
		mcl:    conn,
	}, nil
}

func (s *MongoClient) Ping() error {
	return s.mcl.Ping(context.Background(), readpref.Primary())
}

func (s *MongoClient) SetTTL() (interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return nil, err
	}

	col.Indexes().DropOne(ctx, "TTL")

	opts := &options.IndexOptions{}
	opts.SetName("TTL")
	opts.SetExpireAfterSeconds(s.ExpDur)

	model := mongo.IndexModel{
		Keys: bson.D{
			bson.E{"created_at", 1},
		},
		Options: opts,
	}

	return col.Indexes().CreateOne(ctx, model)
}

// IndexKey holds a key of index
type IndexKey struct {
	Key  string
	Desc bool
}

// Index reprsents a mongodb index
type Index struct {
	Keys   []IndexKey
	Name   string
	Unique bool
	Sparse bool
}

func (i *Index) model() mongo.IndexModel {
	keys := bson.D{}
	for _, k := range i.Keys {
		d := 1
		if k.Desc {
			d = -1
		}
		keys = append(keys, bson.E{k.Key, d})
	}

	opts := &options.IndexOptions{}
	if i.Name != "" {
		opts.SetName(i.Name)
	}
	opts.SetSparse(i.Sparse)
	opts.SetUnique(i.Unique)

	m := mongo.IndexModel{
		Keys:    keys,
		Options: opts,
	}

	return m
}

func (s *MongoClient) EnsureIndices(indices []Index) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return err
	}

	models := []mongo.IndexModel{}
	for _, ind := range indices {
		models = append(models, ind.model())
	}

	if _, err := col.Indexes().CreateMany(ctx, models); err != nil {
		return err
	}

	return nil
}

func (s *MongoClient) DropIndices() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return err
	}

	_, err = col.Indexes().DropAll(ctx)
	return err
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

func (s *MongoClient) Get(q bson.M) (*mongo.SingleResult, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return nil, err
	}

	resp := col.FindOne(ctx, q)
	if resp.Err() != nil {
		return nil, resp.Err()
	}

	return resp, nil
}

func (s *MongoClient) Create(t interface{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return err
	}

	_, err = col.InsertOne(ctx, t)
	return err
}

func (s *MongoClient) Update(q, val interface{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return err
	}

	resp, err := col.ReplaceOne(ctx, q, val)
	if err != nil {
		return err
	}
	if resp.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

func (s *MongoClient) UpdatePartial(q, val interface{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return err
	}

	resp, err := col.UpdateOne(ctx, q, val)
	if err != nil {
		return err
	}
	if resp.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

func (s *MongoClient) List(q interface{}, skip, limit int) (*mongo.Cursor, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return nil, err
	}

	opt := options.Find().SetSkip(int64(skip)).SetLimit(int64(limit))
	cursor, err := col.Find(ctx, q, opt)
	if err != nil {
		return nil, err
	}
	if cursor.Err() != nil {
		return nil, cursor.Err()
	}

	return cursor, nil
}

func (s *MongoClient) Aggregate(q interface{}) (*mongo.Cursor, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := s.getCollection()
	if err != nil {
		return nil, err
	}

	cursor, err := col.Aggregate(ctx, q)
	if err != nil {
		return nil, err
	}
	if cursor.Err() != nil {
		return nil, cursor.Err()
	}

	return cursor, nil
}
