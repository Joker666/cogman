package rest

import (
	"context"
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

	hdlr.mux.HandleFunc("/list", hdlr.listTask)

	return hdlr
}
