package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"

	"github.com/realkinetic/cloud-native-meetup-2019/flight-service/service"
	"github.com/realkinetic/cloud-native-meetup-2019/util"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	hook, err := util.NewContextHook("flight-service")
	if err != nil {
		panic(err)
	}
	log.AddHook(hook)

	tracer := util.InitTracer("car-service", log.StandardLogger())
	opentracing.InitGlobalTracer(tracer)
}

const port = ":8080"

type server struct {
	service service.FlightService
}

func main() {
	flightService, err := service.NewFlightService()
	if err != nil {
		panic(err)
	}
	s := &server{service: flightService}
	http.HandleFunc("/booking", s.bookingHandler)
	log.Infof("Flight service listening on %s...", port)
	mux := nethttp.Middleware(opentracing.GlobalTracer(), http.DefaultServeMux)
	if err := http.ListenAndServe(port, mux); err != nil {
		panic(err)
	}
}

func (s *server) bookingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := util.ContextWithRequest(r.Context(), r)
	switch r.Method {
	case "GET":
		s.getBooking(ctx, w, r)
	case "POST":
		s.bookFlight(ctx, w, r)
	default:
		log.WithContext(ctx).WithFields(log.Fields{
			"error": errors.New("invalid HTTP method"),
		}).Error("Invalid HTTP method for endpoint")
		http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
	}
}

func (s *server) getBooking(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ref := r.URL.Query().Get("ref")
	confirmation, err := s.service.GetBooking(ctx, ref)
	if err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
			"ref":   ref,
		}).Error("Failed to fetch booking")
		if err == service.ErrNoSuchBooking {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	resp, err := json.Marshal(confirmation)
	if err != nil {
		panic(err)
	}

	log.WithContext(ctx).WithFields(log.Fields{
		"ref": ref,
	}).Info("Fetched booking")
	w.Write(resp)
}

func (s *server) bookFlight(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
		}).Error("Failed to read request body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req service.BookFlightRequest
	if err := json.Unmarshal(data, &req); err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
		}).Error("Failed to unmarshal request body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := req.Validate(); err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
		}).Error("Invalid booking request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	confirmation, err := s.service.BookFlight(ctx, &req)
	if err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
		}).Error("Failed to book flight")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp, err := json.Marshal(confirmation)
	if err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to marshal response")
	}

	log.WithContext(ctx).WithFields(log.Fields{
		"ref": confirmation.Ref,
	}).Info("Booked flight")
	w.WriteHeader(http.StatusCreated)
	w.Write(resp)
}
