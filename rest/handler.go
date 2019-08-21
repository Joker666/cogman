package rest

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Tapfury/cogman/client"
	"github.com/Tapfury/cogman/repo"
	cogman "github.com/Tapfury/cogman/repo"
	"github.com/Tapfury/cogman/rest/resp"
	"github.com/Tapfury/cogman/util"
	"github.com/streadway/amqp"
)

type RestConfig struct {
	Port string

	AmqpCon *amqp.Connection
	Clnt    *client.Session

	TaskRep   *repo.TaskRepository
	QueueName []string

	Lgr util.Logger
}

type cogmanHandler struct {
	mux *http.ServeMux

	amqp      *amqp.Connection
	clnt      *client.Session
	taskRepo  *cogman.TaskRepository
	queueName []string

	log util.Logger
}

func (s *cogmanHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO: auth middleware

	s.mux.ServeHTTP(w, r)
}

func NewCogmanHandler(cfg *RestConfig) *cogmanHandler {
	return &cogmanHandler{
		mux: http.NewServeMux(),

		amqp:      cfg.AmqpCon,
		clnt:      cfg.Clnt,
		taskRepo:  cfg.TaskRep,
		queueName: cfg.QueueName,

		log: cfg.Lgr,
	}
}

func (s *cogmanHandler) get(w http.ResponseWriter, r *http.Request) {
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

func (s *cogmanHandler) listTask(w http.ResponseWriter, r *http.Request) {
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

func (s *cogmanHandler) GetDaterangecount(w http.ResponseWriter, r *http.Request) {
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
	TotalQueue int `json: "total_queue"`
	TotalTask  int `json: "total_task"`
	Queue      []queueInfo
}

func (s *cogmanHandler) info(w http.ResponseWriter, r *http.Request) {
	chnl, err := s.amqp.Channel()
	if err != nil {
		resp.ServeError(w, r, err)
		return
	}
	defer chnl.Close()

	data := []queueInfo{}
	totalTask := 0

	for _, q := range s.queueName {
		queue, err := chnl.QueueInspect(q)
		if err != nil {
			data = append(data, queueInfo{q, 0, err.Error()})
			continue
		}

		data = append(data, queueInfo{q, queue.Messages, "OK"})
		totalTask += queue.Messages
	}

	resp.ServeData(w, r, http.StatusOK, amqpInfo{len(data), totalTask, data}, nil)
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
