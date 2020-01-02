package infra

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	database   = "cogman"
	tableTasks = "tasks"
	session_id = "session_id"
)

// MongoClient contaiend required field
type MongoClient struct {
	URL    string
	ExpDur int32
	mcl    *mongo.Client
	sess   sync.Map
}

// NewMongoClient return a mongo client
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

func (m *MongoClient) StartTransaction(ctx context.Context, id string) (context.Context, error) {
	sess, err := m.mcl.StartSession()
	if err != nil {
		return ctx, err
	}
	if err := sess.StartTransaction(); err != nil {
		return ctx, err
	}
	m.sess.Store(id, sess)
	ctx = context.WithValue(ctx,  session_id, id)
	return ctx, nil
}

func (m *MongoClient) CommitTransaction(ctx context.Context) (interface{}, error) {
	id := getSessionID(ctx)
	result, ok := m.sess.Load(id)
	if ok {
		sess := result.(mongo.Session)
		err := sess.CommitTransaction(ctx)
		if err != nil {
			return nil, err
		}
		sess.EndSession(ctx)
		m.sess.Delete(id)
	}
	return nil, nil
}

func (m *MongoClient) AbortTransaction(ctx context.Context) (interface{}, error)  {
	id := getSessionID(ctx)
	result, ok := m.sess.Load(id)
	if ok {
		sess := result.(mongo.Session)
		err := sess.AbortTransaction(ctx)
		if err != nil {
			return nil, err
		}
		sess.EndSession(ctx)
		m.sess.Delete(id)
	}
	return nil, nil
}

// Ping check the mongo connection status
func (m *MongoClient) Ping() error {
	return m.mcl.Ping(context.Background(), readpref.Primary())
}

// SetTTL for mongo object
func (m *MongoClient) SetTTL() (interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := m.getCollection(m.mcl)
	if err != nil {
		return nil, err
	}

	_, _ = col.Indexes().DropOne(ctx, "TTL")

	opts := &options.IndexOptions{}
	opts.SetName("TTL")
	opts.SetExpireAfterSeconds(m.ExpDur)

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

// EnsureIndices ensure mongo index list
func (m *MongoClient) EnsureIndices(indices []Index) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := m.getCollection(m.mcl)
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

// DropIndices drop previous mongo field
func (m *MongoClient) DropIndices() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	col, err := m.getCollection(m.mcl)
	if err != nil {
		return err
	}

	_, err = col.Indexes().DropAll(ctx)
	return err
}

// Close close the mongo connection
func (m *MongoClient) Close() error {
	return m.mcl.Disconnect(context.Background())
}

// Connect initiate a mongo connection
func (m *MongoClient) Connect() error {
	err := m.mcl.Connect(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoClient) getClient(ctx context.Context) (*mongo.Client, mongo.Session, bool) {
	tid := getSessionID(ctx)
	val, ok := m.sess.Load(tid)
	if !ok {
		return m.mcl, nil, false
	}
	if val != nil {
		sess := val.(mongo.Session)
		return sess.Client(), sess, true
	}
	return m.mcl, nil, false
}

func (m *MongoClient) getCollection(client *mongo.Client) (*mongo.Collection, error) {
	return client.Database(database).Collection(tableTasks), nil
}

// Get return a single object based on query parameter
func (m *MongoClient) Get(ctx context.Context, q bson.M) (*mongo.SingleResult, error) {
	client, sess, withSession := m.getClient(ctx)

	col, err := m.getCollection(client)
	if err != nil {
		return nil, err
	}
	var resp *mongo.SingleResult
	if withSession {
		err = mongo.WithSession(ctx, sess, func(sessionContext mongo.SessionContext) error {
			resp = col.FindOne(sessionContext, q)
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		resp = col.FindOne(ctx, q)
	}
	if resp.Err() != nil {
		return nil, resp.Err()
	}

	return resp, nil
}

// Create create a object
func (m *MongoClient) Create(ctx context.Context, t interface{}) error {
	client, sess, withSession := m.getClient(ctx)

	col, err := m.getCollection(client)
	if err != nil {
		return err
	}

	if withSession {
		err = mongo.WithSession(ctx, sess, func(sessionContext mongo.SessionContext) error {
			_, err = col.InsertOne(sessionContext, t)
			return err
		})
	}else {
		_, err = col.InsertOne(ctx, t)
	}
	return err
}

// Update update a object
func (m *MongoClient) Update(ctx context.Context, q, val interface{}) error {
	client, sess, withSession := m.getClient(ctx)
	col, err := m.getCollection(client)
	if err != nil {
		return err
	}

	var resp *mongo.UpdateResult
	if withSession {
		err = mongo.WithSession(ctx, sess, func(sessionContext mongo.SessionContext) error {
			resp, err = col.ReplaceOne(sessionContext, q, val)
			return err
		})
	}else {
		resp, err = col.ReplaceOne(ctx, q, val)
	}
	if err != nil {
		return err
	}
	if resp.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// UpdatePartial update a object partially
func (m *MongoClient) UpdatePartial(ctx context.Context, q, val interface{}) error {
	client, sess, withSession := m.getClient(ctx)

	col, err := m.getCollection(client)
	if err != nil {
		return err
	}

	var resp *mongo.UpdateResult
	if withSession {
		err = mongo.WithSession(ctx, sess, func(sessionContext mongo.SessionContext) error {
			resp, err = col.UpdateOne(sessionContext, q, val)
			return err
		})
	} else {
		resp, err = col.UpdateOne(ctx, q, val)
	}
	if err != nil {
		return err
	}
	if resp.MatchedCount == 0 {
		return ErrNotFound
	}

	return nil
}

// List return a list of object based on query parameter
func (m *MongoClient) List(ctx context.Context, q interface{}, skip, limit int) (*mongo.Cursor, error) {
	client, sess, withSession := m.getClient(ctx)

	col, err := m.getCollection(client)
	if err != nil {
		return nil, err
	}
	opt := options.Find().SetSkip(int64(skip)).SetLimit(int64(limit))

	var cursor *mongo.Cursor
	if withSession {
		err = mongo.WithSession(ctx, sess, func(sessionContext mongo.SessionContext) error {
			cursor, err = col.Find(sessionContext, q, opt)
			return err
		})
	} else {
		cursor, err = col.Find(ctx, q, opt)
	}
	if err != nil {
		return nil, err
	}
	if cursor.Err() != nil {
		return nil, cursor.Err()
	}

	return cursor, nil
}

// Aggregate return a Cursor
func (m *MongoClient) Aggregate(ctx context.Context, q interface{}) (*mongo.Cursor, error) {
	client, sess, withSession := m.getClient(ctx)

	col, err := m.getCollection(client)
	if err != nil {
		return nil, err
	}

	var cursor *mongo.Cursor
	if withSession {
		err = mongo.WithSession(ctx,sess, func(sessionContext mongo.SessionContext) error {
			cursor, err = col.Aggregate(sessionContext, q)
			return err
		})
	} else {
		cursor, err = col.Aggregate(ctx, q)
	}
	if err != nil {
		return nil, err
	}
	if cursor.Err() != nil {
		return nil, cursor.Err()
	}

	return cursor, nil
}


func getSessionID(ctx context.Context) string {
	val := ctx.Value(session_id)
	if val == nil {
		return ""
	}
	return val.(string)
}
