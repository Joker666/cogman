package resp

import (
	"net/http"

	"github.com/Tapfury/phoneline-api/util"
)

// Error represents a response object of api error
type Error struct {
	ID         string                 `json:"id,omitempty"`
	Message    string                 `json:"message,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
	StackTrace string                 `json:"stackTrace,omitempty"`
}

// ServeBadRequest serves http BadRequest
func ServeBadRequest(w http.ResponseWriter, r *http.Request, err error) {
	re := &JSONResponse{
		response: response{code: http.StatusBadRequest},
		Errors: []Error{
			{
				ID:      util.GenerateRandStr(10),
				Message: err.Error(),
			},
		},
	}
	Render(w, r, re)
}

// ServeUnauthorized serves http Unauthorized
func ServeUnauthorized(w http.ResponseWriter, r *http.Request, err error) {
	re := &JSONResponse{
		response: response{code: http.StatusUnauthorized},
		Errors: []Error{
			{
				ID:      util.GenerateRandStr(10),
				Message: err.Error(),
			},
		},
	}
	Render(w, r, re)
}

// ServeForbidden serves http Forbidden
func ServeForbidden(w http.ResponseWriter, r *http.Request, err error) {
	re := &JSONResponse{
		response: response{code: http.StatusForbidden},
		Errors: []Error{
			{
				ID:      util.GenerateRandStr(10),
				Message: err.Error(),
			},
		},
	}
	Render(w, r, re)
}

// ServeInternalServerError serves http InternalServerError
func ServeInternalServerError(w http.ResponseWriter, r *http.Request, err error) {
	re := &JSONResponse{
		response: response{code: http.StatusInternalServerError},
		Errors: []Error{
			{
				ID:      util.GenerateRandStr(10),
				Message: err.Error(),
			},
		},
	}
	Render(w, r, re)
}
