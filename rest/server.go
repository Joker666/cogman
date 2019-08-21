package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

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

func getHandler(cfg *RestConfig) http.Handler {
	hdlr := NewCogmanHandler(cfg)

	hdlr.mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Hello, Cogman alive!!!")
	})

	hdlr.mux.HandleFunc("/get", hdlr.get)
	hdlr.mux.HandleFunc("/list", hdlr.listTask)
	hdlr.mux.HandleFunc("/daterangecount", hdlr.GetDaterangecount)
	hdlr.mux.HandleFunc("/info", hdlr.info)

	return hdlr
}
