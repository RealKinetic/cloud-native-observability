package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/nats-io/nuid"
	"github.com/opentracing-contrib/go-aws-sdk"

	cars "github.com/realkinetic/cloud-native-meetup-2019/car-service/service"
	flights "github.com/realkinetic/cloud-native-meetup-2019/flight-service/service"
	hotels "github.com/realkinetic/cloud-native-meetup-2019/hotel-service/service"
	"github.com/realkinetic/cloud-native-meetup-2019/util"
)

const (
	flightServiceURLEnv = "FLIGHT_SERVICE_URL"
	hotelServiceURLEnv  = "HOTEL_SERVICE_URL"
	carServiceURLEnv    = "CAR_SERVICE_URL"
)

var (
	ErrNoSuchBooking = errors.New("no such booking")
	tripsTable       = "trips"
	flightServiceURL = os.Getenv(flightServiceURLEnv)
	hotelServiceURL  = os.Getenv(hotelServiceURLEnv)
	carServiceURL    = os.Getenv(carServiceURLEnv)
)

type TripConfirmation struct {
	Ref                   string                      `json:"ref"`
	Trip                  *BookTripRequest            `json:"trip"`
	FlightConfirmation    *flights.FlightConfirmation `json:"flight_confirmation,omitempty"`
	HotelConfirmation     *hotels.HotelConfirmation   `json:"hotel_confirmation,omitempty"`
	CarRentalConfirmation *cars.CarRentalConfirmation `json:"car_rental_confirmation,omitempty"`
}

type TripBooking struct {
	Request   *BookTripRequest `json:"request"`
	Created   time.Time        `json:"created"`
	Ref       string           `json:"ref"`
	FlightRef string           `json:"flight_ref"`
	HotelRef  string           `json:"hotel_ref"`
	CarRef    string           `json:"car_ref"`
}

type BookTripRequest struct {
	Name        string                     `json:"name"`
	TripName    string                     `json:"trip_name"`
	Destination string                     `json:"destination"`
	Start       time.Time                  `json:"start"`
	End         time.Time                  `json:"end"`
	Members     []string                   `json:"members"`
	Flight      *flights.BookFlightRequest `json:"flight,omitempty"`
	Hotel       *hotels.BookHotelRequest   `json:"hotel,omitempty"`
	Car         *cars.BookCarRentalRequest `json:"car,omitempty"`
}

func (b *BookTripRequest) Validate() error {
	if b.Name == "" {
		return errors.New("invalid name")
	}
	if b.Destination == "" {
		return errors.New("invalid destination")
	}
	if b.Start.IsZero() {
		return errors.New("invalid start date")
	}
	if b.End.IsZero() {
		return errors.New("invalid end date")
	}
	if len(b.Members) == 0 {
		return errors.New("invalid members")
	}
	for _, m := range b.Members {
		if len(m) == 0 {
			return errors.New("invalid member name")
		}
	}
	if b.Flight != nil {
		if err := b.Flight.Validate(); err != nil {
			return err
		}
	}
	if b.Hotel != nil {
		if err := b.Hotel.Validate(); err != nil {
			return err
		}
	}
	if b.Car != nil {
		if err := b.Car.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type TripService interface {
	BookTrip(context.Context, *BookTripRequest) (*TripConfirmation, error)
	GetBooking(ctx context.Context, ref string) (*TripConfirmation, error)
}

type dynamoService struct {
	db         *dynamodb.DynamoDB
	httpClient *http.Client
}

func NewTripService() (TripService, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String("us-east-1")},
	}))
	db := dynamodb.New(sess)
	otaws.AddOTHandlers(db.Client)

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ref"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ref"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(2),
			WriteCapacityUnits: aws.Int64(2),
		},
		TableName: aws.String(tripsTable),
	}
	_, err := db.CreateTable(input)
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok {
			if awsError.Code() != dynamodb.ErrCodeResourceInUseException {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return &dynamoService{db: db, httpClient: util.NewHTTPClient()}, nil
}

func (d *dynamoService) BookTrip(ctx context.Context, r *BookTripRequest) (*TripConfirmation, error) {
	ref := nuid.Next()
	confirmation := &TripConfirmation{Ref: ref, Trip: r}
	trip := &TripBooking{
		Request: r,
		Ref:     ref,
		Created: time.Now(),
	}
	if r.Flight != nil {
		flightConfirmation, err := d.bookFlight(ctx, r.Flight)
		if err != nil {
			return nil, err
		}
		confirmation.FlightConfirmation = flightConfirmation
		trip.FlightRef = flightConfirmation.Ref
	}
	if r.Hotel != nil {
		hotelConfirmation, err := d.bookHotel(ctx, r.Hotel)
		if err != nil {
			return nil, err
		}
		confirmation.HotelConfirmation = hotelConfirmation
		trip.HotelRef = hotelConfirmation.Ref
	}
	if r.Car != nil {
		carConfirmation, err := d.bookCar(ctx, r.Car)
		if err != nil {
			return nil, err
		}
		confirmation.CarRentalConfirmation = carConfirmation
		trip.CarRef = carConfirmation.Ref
	}

	// Don't store these since it's redundant.
	r.Flight = nil
	r.Hotel = nil
	r.Car = nil

	av, err := dynamodbattribute.MarshalMap(trip)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tripsTable),
	}
	_, err = d.db.PutItemWithContext(ctx, input)

	return confirmation, err
}

func (d *dynamoService) GetBooking(ctx context.Context, ref string) (*TripConfirmation, error) {
	result, err := d.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tripsTable),
		Key: map[string]*dynamodb.AttributeValue{
			"ref": {
				S: aws.String(ref),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	var trip *TripBooking
	if err := dynamodbattribute.UnmarshalMap(result.Item, &trip); err != nil {
		return nil, err
	}
	if trip.Ref == "" {
		return nil, ErrNoSuchBooking
	}
	confirmation := &TripConfirmation{Ref: ref, Trip: trip.Request}

	if trip.FlightRef != "" {
		flight, err := d.getFlight(ctx, trip.FlightRef)
		if err != nil {
			return nil, err
		}
		confirmation.FlightConfirmation = flight
	}
	if trip.HotelRef != "" {
		hotel, err := d.getHotel(ctx, trip.HotelRef)
		if err != nil {
			return nil, err
		}
		confirmation.HotelConfirmation = hotel
	}
	if trip.CarRef != "" {
		car, err := d.getCar(ctx, trip.CarRef)
		if err != nil {
			return nil, err
		}
		confirmation.CarRentalConfirmation = car
	}

	return confirmation, err
}

func (d *dynamoService) getFlight(ctx context.Context, ref string) (*flights.FlightConfirmation, error) {
	var confirmation *flights.FlightConfirmation
	err := d.getBooking(ctx, fmt.Sprintf("%s/flights/booking?ref=%s", flightServiceURL, ref), &confirmation)
	return confirmation, err
}

func (d *dynamoService) getHotel(ctx context.Context, ref string) (*hotels.HotelConfirmation, error) {
	var confirmation *hotels.HotelConfirmation
	err := d.getBooking(ctx, fmt.Sprintf("%s/hotels/booking?ref=%s", hotelServiceURL, ref), &confirmation)
	return confirmation, err
}

func (d *dynamoService) getCar(ctx context.Context, ref string) (*cars.CarRentalConfirmation, error) {
	var confirmation *cars.CarRentalConfirmation
	err := d.getBooking(ctx, fmt.Sprintf("%s/cars/booking?ref=%s", carServiceURL, ref), &confirmation)
	return confirmation, err
}

func (d *dynamoService) getBooking(ctx context.Context, url string, returned interface{}) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GetBooking request returned status code %d (%s)", resp.StatusCode, data)
	}
	return json.Unmarshal(data, &returned)
}

func (d *dynamoService) bookFlight(ctx context.Context, r *flights.BookFlightRequest) (*flights.FlightConfirmation, error) {
	var confirmation *flights.FlightConfirmation
	err := d.book(ctx, r, flightServiceURL+"/flights/booking", &confirmation)
	return confirmation, err
}

func (d *dynamoService) bookHotel(ctx context.Context, r *hotels.BookHotelRequest) (*hotels.HotelConfirmation, error) {
	var confirmation *hotels.HotelConfirmation
	err := d.book(ctx, r, hotelServiceURL+"/hotels/booking", &confirmation)
	return confirmation, err
}

func (d *dynamoService) bookCar(ctx context.Context, r *cars.BookCarRentalRequest) (*cars.CarRentalConfirmation, error) {
	var confirmation *cars.CarRentalConfirmation
	err := d.book(ctx, r, carServiceURL+"/cars/booking", &confirmation)
	return confirmation, err
}

func (d *dynamoService) book(ctx context.Context, payload interface{}, url string, returned interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("%s request returned status code %d (%s)", url, resp.StatusCode, data)
	}
	return json.Unmarshal(data, &returned)
}
