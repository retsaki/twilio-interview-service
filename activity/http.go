package activity

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

type recordResp struct {
	OK bool `json:"ok"`
}

type countResp struct {
	UserID     string `json:"userID"`
	ActionType string `json:"actionType"`
	Hours      int    `json:"hours"`
	Count      int64  `json:"count"`
}

type statusResp struct {
	UserID    string `json:"userID"`
	Hours     int    `json:"hours"`
	Threshold int    `json:"threshold"`
	Total     int64  `json:"total"`
	Status    string `json:"status"`
}

func AttachHTTPHandlers(mux *http.ServeMux, svc *Service) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("POST /action", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var a Action
		if err := dec.Decode(&a); err != nil {
			var syn *json.SyntaxError
			if errors.As(err, &syn) || errors.Is(err, io.ErrUnexpectedEOF) {
				http.Error(w, "malformed json", http.StatusBadRequest)
				return
			}
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := svc.RecordAction(ctx, a); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusCreated, recordResp{OK: true})
	})

	mux.HandleFunc("GET /action/count", func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("userID")
		atype := r.URL.Query().Get("actionType")
		hours := parseIntOr(r.URL.Query().Get("hours"), svc.config.DefaultStatusWindowHours)
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		n, err := svc.CountAction(ctx, user, atype, hours)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, countResp{UserID: user, ActionType: atype, Hours: hours, Count: n})
	})

	mux.HandleFunc("GET /user/status", func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("userID")
		hours := parseIntOr(r.URL.Query().Get("hours"), svc.config.DefaultStatusWindowHours)
		thr := parseIntOr(r.URL.Query().Get("threshold"), svc.config.DefaultThreshold)
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		status, total, err := svc.UserStatus(ctx, user, hours, thr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, statusResp{UserID: user, Hours: hours, Threshold: thr, Total: total, Status: status})
	})
}

func parseIntOr(s string, def int) int {
	if s == "" {
		return def
	}
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}
	return def
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
