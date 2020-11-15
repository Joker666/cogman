package resp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Response returns response body
type Response interface {
	Header() http.Header
	Body() []byte
	StatusCode() int
}

type response struct {
	header http.Header
	code   int
	body   []byte
}

func (r *response) SetHeader(hdr http.Header) {
	r.header = hdr
}

func (r *response) SetStatusCode(code int) {
	r.code = code
}

func (r *response) Header() http.Header {
	return r.header
}

func (r *response) Body() []byte {
	return r.body
}

func (r *response) StatusCode() int {
	return r.code
}

// JSONResponse represents the root response object of api response
type JSONResponse struct {
	response
	Data   interface{} `json:"data,omitempty"`
	Errors []Error     `json:"errors,omitempty"`
	Meta   *Pager      `json:"meta,omitempty"`
}

// Header returns header
func (r *JSONResponse) Header() http.Header {
	if r.header == nil {
		r.header = http.Header{}
	}
	r.header.Set("Content-Type", "application/json")
	return r.header
}

// Body returns payload
func (r *JSONResponse) Body() []byte {
	body, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return body
}

// Render renders Response
func Render(w http.ResponseWriter, r *http.Request, resp Response) {
	for k, vs := range resp.Header() {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	if sc := resp.StatusCode(); sc == 0 {
		panic(errors.New("response status not defined"))
	} else {
		w.WriteHeader(sc)
	}
	if _, err := w.Write(resp.Body()); err != nil {
		panic(err)
	}
}

// FormatTime formats response time
func FormatTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

// ServeData serves data and meta with http status code 2xx
func ServeData(w http.ResponseWriter, r *http.Request, code int, data interface{}, meta *Pager) {
	if code < 200 || code > 299 {
		panic(fmt.Errorf("resp: serve data with status %d", code))
	}
	re := &JSONResponse{
		response: response{code: code},
		Data:     data,
		Meta:     meta,
	}
	Render(w, r, re)
}
