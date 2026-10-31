// Package httpapi adapts api.Service to HTTP/JSON.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
)

const maxBodyBytes = 64 << 20

type errorBody struct {
	Error *api.Error `json:"error"`
}

// NewHandler returns the HTTP routes for svc.
func NewHandler(svc api.Service) http.Handler {
	mux := http.NewServeMux()
	Register(mux, svc)
	return mux
}

// Register adds the public API routes for svc to mux.
func Register(mux *http.ServeMux, svc api.Service) {
	h := &handler{svc: svc}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/broker", func(w http.ResponseWriter, r *http.Request) { WriteJSON(w, http.StatusOK, svc.Info()) })
	mux.HandleFunc("GET /v1/topics", h.listTopics)
	mux.HandleFunc("POST /v1/topics", h.createTopic)
	mux.HandleFunc("GET /v1/topics/{topic}", h.getTopic)
	mux.HandleFunc("POST /v1/topics/{topic}/records", h.produce)
	mux.HandleFunc("GET /v1/topics/{topic}/partitions/{partition}/records", h.fetch)
}

type handler struct{ svc api.Service }

func (h *handler) listTopics(w http.ResponseWriter, r *http.Request) {
	topics, err := h.svc.Topics(r.Context())
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]any{"topics": topics})
}

func (h *handler) createTopic(w http.ResponseWriter, r *http.Request) {
	var cfg api.TopicConfig
	if !Decode(w, r, &cfg) {
		return
	}
	info, err := h.svc.CreateTopic(r.Context(), cfg)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, info)
}

func (h *handler) getTopic(w http.ResponseWriter, r *http.Request) {
	info, err := h.svc.Topic(r.Context(), r.PathValue("topic"))
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, info)
}

func (h *handler) produce(w http.ResponseWriter, r *http.Request) {
	var req api.ProduceRequest
	if !Decode(w, r, &req) {
		return
	}
	req.Topic = r.PathValue("topic")
	resp, err := h.svc.Produce(r.Context(), req)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, resp)
}

func (h *handler) fetch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	req := api.FetchRequest{Topic: r.PathValue("topic")}
	var err error
	if req.Partition, err = strconv.Atoi(r.PathValue("partition")); err != nil {
		WriteError(w, api.Errorf(api.CodeInvalidRequest, "bad partition %q", r.PathValue("partition")))
		return
	}
	for name, dst := range map[string]*int{"max_records": &req.MaxRecords, "max_bytes": &req.MaxBytes} {
		if s := q.Get(name); s != "" {
			if *dst, err = strconv.Atoi(s); err != nil || *dst < 0 {
				WriteError(w, api.Errorf(api.CodeInvalidRequest, "bad %s %q", name, s))
				return
			}
		}
	}
	if s := q.Get("offset"); s != "" {
		if req.Offset, err = strconv.ParseUint(s, 10, 64); err != nil {
			WriteError(w, api.Errorf(api.CodeInvalidRequest, "bad offset %q", s))
			return
		}
	}
	if s := q.Get("wait_ms"); s != "" {
		ms, err := strconv.Atoi(s)
		if err != nil || ms < 0 {
			WriteError(w, api.Errorf(api.CodeInvalidRequest, "bad wait_ms %q", s))
			return
		}
		req.Wait = time.Duration(ms) * time.Millisecond
	}
	resp, err := h.svc.Fetch(r.Context(), req)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, resp)
}

func Decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		WriteError(w, api.Errorf(api.CodeInvalidRequest, "bad JSON body: %v", err))
		return false
	}
	return true
}

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func WriteError(w http.ResponseWriter, err error) {
	var e *api.Error
	if !errors.As(err, &e) {
		e = &api.Error{Code: api.CodeInternal, Message: err.Error()}
	}
	WriteJSON(w, e.Code.HTTPStatus(), errorBody{Error: e})
}
