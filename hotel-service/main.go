package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/realkinetic/cloud-native-meetup-2019/hotel-service/service"
	"github.com/realkinetic/cloud-native-meetup-2019/util"
)

const port = ":8081"

var notrace = flag.Bool("notrace", false, "disable tracing")

type server struct {
	service service.HotelService
}

func main() {
	flag.Parse()
	if err := util.Init("hotel-service", *notrace); err != nil {
		panic(err)
	}

	hotelService, err := service.NewHotelService()
	if err != nil {
		panic(err)
	}

	s := &server{service: hotelService}
	http.HandleFunc("/hotels/booking", s.bookingHandler)
	handler := util.NewContextHandler(http.DefaultServeMux)

	log.Infof("Hotel service listening on %s...", port)
	if err := http.ListenAndServe(port, handler); err != nil {
		panic(err)
	}
}

func (s *server) bookingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	switch r.Method {
	case "GET":
		s.getBooking(ctx, w, r)
	case "POST":
		s.bookHotel(ctx, w, r)
	default:
		log.WithContext(ctx).WithFields(log.Fields{
			"error": errors.New("invalid HTTP method"),
		}).Error("Invalid HTTP method for endpoint")
		http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
	}
}

func (s *server) getBooking(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ref := r.URL.Query().Get("ref")
	ctx = util.WithRef(ctx, ref)
	confirmation, err := s.service.GetBooking(ctx, ref)
	if err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
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

	log.WithContext(ctx).Info("Fetched booking")
	w.Write(resp)
}

func (s *server) bookHotel(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
		}).Error("Failed to read request body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req service.BookHotelRequest
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

	confirmation, err := s.service.BookHotel(ctx, &req)
	if err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
		}).Error("Failed to book hotel")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp, err := json.Marshal(confirmation)
	if err != nil {
		log.WithContext(ctx).WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to marshal response")
	}

	log.WithContext(ctx).Info("Booked hotel")
	w.WriteHeader(http.StatusCreated)
	w.Write(resp)
}
