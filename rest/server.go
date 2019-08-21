package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	cogman "github.com/Tapfury/cogman/repo"
	"github.com/Tapfury/cogman/util"
)

func StartRestServer(ctx context.Context, port string, taskRep *cogman.TaskRepository, lgr util.Logger) {
	if port == "" {
		port = ":8081"
	}

	server := &http.Server{
		Addr:         port,
		Handler:      getHandler(taskRep, lgr),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	errorChan := make(chan error, 1)

	go func() {
		if err := server.ListenAndServe(); err != nil {
			errorChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		lgr.Info("rest server: server closing")
	case err := <-errorChan:
		lgr.Error("rest server: error found", err)
	}

	_ = server.Shutdown(ctx)
}

func getHandler(taskRep *cogman.TaskRepository, lgr util.Logger) http.Handler {
	hdlr := NewCogmanHandler(taskRep, lgr)

	hdlr.mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Hello, Cogman alive!!!")
	})

	hdlr.mux.HandleFunc("/get", hdlr.get)
	hdlr.mux.HandleFunc("/list", hdlr.listTask)
	hdlr.mux.HandleFunc("/daterangecount", hdlr.GetDaterangecount)

	return hdlr
}
