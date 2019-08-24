package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// StartRestServer intiate a rest server
func StartRestServer(ctx context.Context, cfg *RestConfig) {
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
func getHandler(cfg *RestConfig) http.Handler {
	hdlr := NewCogmanHandler(cfg)

	hdlr.mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Hello, Cogman alive!!!")
	})

	hdlr.mux.HandleFunc("/get", hdlr.get)
	hdlr.mux.HandleFunc("/list", hdlr.listTask)
	hdlr.mux.HandleFunc("/daterangecount", hdlr.getDaterangecount)
	hdlr.mux.HandleFunc("/info", hdlr.info)
	hdlr.mux.HandleFunc("/retry", hdlr.retry)

	return hdlr
}
