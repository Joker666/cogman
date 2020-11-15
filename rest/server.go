package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// StartRestServer initiates a rest server
func StartRestServer(ctx context.Context, cfg *Config) {
	if cfg.Port == "" {
		cfg.Port = ":8081"
	}

	server := &http.Server{
		Addr:         cfg.Port,
		Handler:      getHandler(cfg),
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
		cfg.Lgr.Info("rest server: server closing")
	case err := <-errorChan:
		cfg.Lgr.Error("rest server: error found", err)
	}

	_ = server.Shutdown(ctx)
}

// list of route
func getHandler(cfg *Config) http.Handler {
	handler := NewCogmanHandler(cfg)

	handler.mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "Hello, Cogman alive!!!")
	})

	handler.mux.HandleFunc("/get", handler.get)
	handler.mux.HandleFunc("/list", handler.listTask)
	handler.mux.HandleFunc("/daterangecount", handler.getDateRangeCount)
	handler.mux.HandleFunc("/info", handler.info)
	handler.mux.HandleFunc("/retry", handler.retry)

	return handler
}
