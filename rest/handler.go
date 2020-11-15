package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Joker666/cogman/client"
	"github.com/Joker666/cogman/repo"
	cogman "github.com/Joker666/cogman/repo"
	"github.com/Joker666/cogman/rest/resp"
	"github.com/Joker666/cogman/util"
	"github.com/streadway/amqp"
)

// Config hold the required fields
type Config struct {
	Port string

	AmqpCon *amqp.Connection
	Client  *client.Session

	TaskRep   *repo.TaskRepository
	QueueName []string

	Lgr util.Logger
}

// CogmanHandler holds necessary fields for handling
type CogmanHandler struct {
	mux *http.ServeMux

	amqp      *amqp.Connection
	client    *client.Session
	taskRepo  *cogman.TaskRepository
	queueName []string

	log util.Logger
}

func (s *CogmanHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO: auth middleware

	s.mux.ServeHTTP(w, r)
}

// NewCogmanHandler return a cogmanHandler instance
func NewCogmanHandler(cfg *Config) *CogmanHandler {
	return &CogmanHandler{
		mux: http.NewServeMux(),

		amqp:      cfg.AmqpCon,
		client:    cfg.Client,
		taskRepo:  cfg.TaskRep,
		queueName: cfg.QueueName,

		log: cfg.Lgr,
	}
}

func setupResponse(w *http.ResponseWriter, req *http.Request) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
	(*w).Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	(*w).Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
}

// get response with a task object
func (s *CogmanHandler) get(w http.ResponseWriter, r *http.Request) {
	setupResponse(&w, r)
	if r.Method != http.MethodGet {
		resp.ServeInvalidMethod(w, r, ErrInvalidMethod)
		return
	}

	q := r.URL.Query()
	taskID := q.Get("task_id")
	if taskID == "" {
		resp.ServeError(w, r, ErrTaskIDRequired)
		return
	}

	task, err := s.taskRepo.GetTask(taskID)
	if err != nil {
		if err == cogman.ErrTaskNotFound {
			resp.ServeNotFound(w, r, ErrTaskNotFound)
			return
		}
		resp.ServeError(w, r, err)
		return
	}

	resp.ServeData(w, r, http.StatusOK, task, nil)
}

// listTask response with a list of task object
func (s *CogmanHandler) listTask(w http.ResponseWriter, r *http.Request) {
	setupResponse(&w, r)
	if r.Method != http.MethodGet {
		resp.ServeInvalidMethod(w, r, ErrInvalidMethod)
		return
	}

	skip, limit := parseSkipLimit(r, 10, 10)
	v, err := parseValues(r)
	if err != nil {
		resp.ServeError(w, r, err)
		return
	}

	startTime, endTime, err := parseTimeRange(r)
	if err != nil {
		resp.ServeError(w, r, err)
		return
	}

	taskList, err := s.taskRepo.List(v, startTime, endTime, skip, limit)
	if err != nil {
		resp.ServeError(w, r, err)
		return
	}

	resp.ServeData(w, r, http.StatusOK, taskList, nil)
}

// getDateRangeCount response with a bucket list of time range
func (s *CogmanHandler) getDateRangeCount(w http.ResponseWriter, r *http.Request) {
	setupResponse(&w, r)
	if r.Method != http.MethodGet {
		resp.ServeInvalidMethod(w, r, ErrInvalidMethod)
		return
	}

	v, err := parseDateRangeFilterValues(r)
	if err != nil {
		resp.ServeError(w, r, err)
		return
	}
	std, _ := v["startDate"].(time.Time)
	etd, _ := v["endDate"].(time.Time)
	itv, _ := v["interval"].(int)

	if std.After(etd) {
		resp.ServeError(w, r, ErrInvalidTimeRange)
		return
	}

	tasks, err := s.taskRepo.ListCountDateRangeInterval(std, etd, itv)
	if err != nil {
		resp.ServeError(w, r, err)
		return
	}

	resp.ServeData(w, r, http.StatusOK, tasks, nil)
}

type queueInfo struct {
	Name   string
	Task   int
	Status string
}

type amqpInfo struct {
	TotalQueue int `json:"total_queue"`
	TotalTask  int `json:"total_task"`
	Queue      []queueInfo
}

// info response the queue list and their info
func (s *CogmanHandler) info(w http.ResponseWriter, r *http.Request) {
	setupResponse(&w, r)
	if r.Method != http.MethodGet {
		resp.ServeInvalidMethod(w, r, ErrInvalidMethod)
		return
	}

	channel, err := s.amqp.Channel()
	if err != nil {
		resp.ServeError(w, r, err)
		return
	}
	defer channel.Close()

	data := []queueInfo{}
	totalTask := 0

	for _, q := range s.queueName {
		queue, err := channel.QueueInspect(q)
		if err != nil {
			data = append(data, queueInfo{q, 0, err.Error()})
			continue
		}

		data = append(data, queueInfo{q, queue.Messages, "OK"})
		totalTask += queue.Messages
	}

	resp.ServeData(w, r, http.StatusOK, amqpInfo{len(data), totalTask, data}, nil)
}

// TaskRetry holds necessary values to retry a task
type TaskRetry struct {
	TaskID string `json:"task_id"`
	Retry  int    `json:"retry"`
}

// retry will re initiate a fail job
func (s *CogmanHandler) retry(w http.ResponseWriter, r *http.Request) {
	setupResponse(&w, r)
	if r.Method != http.MethodPost {
		resp.ServeInvalidMethod(w, r, ErrInvalidMethod)
		return
	}

	rTask := TaskRetry{}
	err := parseJSON(r.Body, &rTask)
	if err != nil {
		resp.ServeBadRequest(w, r, err)
		return
	}
	if rTask.TaskID == "" {
		resp.ServeBadRequest(w, r, ErrTaskIDRequired)
		return
	}

	task, err := s.taskRepo.GetTask(rTask.TaskID)
	if err != nil {
		if err == cogman.ErrTaskNotFound {
			resp.ServeNotFound(w, r, ErrTaskNotFound)
			return
		}
		resp.ServeError(w, r, err)
		return
	}

	if rTask.Retry == 0 {
		rTask.Retry = 1
	}

	s.taskRepo.UpdateRetryCount(task.OriginalTaskID, rTask.Retry)
	if err := s.client.RetryTask(*task); err != nil {
		resp.ServeError(w, r, err)
		return
	}

	resp.ServeData(w, r, http.StatusOK, "OK", nil)
}

func parseJSON(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}

func parseValues(r *http.Request) (map[string]interface{}, error) {
	q := r.URL.Query()
	v := make(map[string]interface{})

	if val := q.Get("task_id"); val != "" {
		v["task_id"] = val
	}
	if val := q.Get("status"); val != "" {
		v["status"] = val
	}
	if val := q.Get("primary_key"); val != "" {
		v["primary_key"] = val
	}

	return v, nil
}

// ParseTimeRange parse start time & end time to search over created_at field
func parseTimeRange(r *http.Request) (*time.Time, *time.Time, error) {
	q := r.URL.Query()

	strTime := q.Get("startDate")
	endTime := q.Get("endDate")

	if strTime == "" && endTime == "" {
		return nil, nil, nil
	}

	if strTime == "" || endTime == "" {
		return nil, nil, ErrBothStartEndTimeRequired
	}

	timeA, err := time.Parse(time.RFC3339, strTime)
	if err != nil {
		return nil, nil, ErrInvalidData
	}

	timeB, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return nil, nil, ErrInvalidData
	}

	if timeA.After(timeB) {
		return nil, nil, ErrInvalidTimeRange
	}

	return &timeA, &timeB, nil
}

func parseSkipLimit(r *http.Request, def, max int) (int, int) {
	q := r.URL.Query()
	skip, _ := strconv.Atoi(q.Get("skip"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit == 0 {
		limit = def
	}
	if limit > max {
		limit = max
	}
	if skip < 0 {
		skip = 0
	}
	return skip, limit
}

func parseDateRangeFilterValues(r *http.Request) (map[string]interface{}, error) {
	q := r.URL.Query()
	v := make(map[string]interface{})

	if val := q.Get("startDate"); val != "" {
		t, err := time.Parse(time.RFC3339, val)
		if err != nil {
			return nil, ErrInvalidData
		}
		v["startDate"] = t
	} else {
		v["startDate"] = time.Now().AddDate(0, 0, -30)
	}

	if val := q.Get("endDate"); val != "" {
		t, err := time.Parse(time.RFC3339, val)
		if err != nil {
			return nil, ErrInvalidData
		}
		v["endDate"] = t
	} else {
		v["endDate"] = time.Now()
	}

	if val := q.Get("interval"); val != "" {
		interval, err := strconv.Atoi(val)
		if err != nil {
			return nil, ErrInvalidData
		}
		if interval <= 0 {
			return nil, ErrInvalidData
		}
		v["interval"] = interval
	} else {
		v["interval"] = 60 * 24 // 1 day
	}

	return v, nil
}
