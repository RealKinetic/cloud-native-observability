package service

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/nats-io/nuid"
	"github.com/opentracing-contrib/go-aws-sdk"
	"github.com/opentracing/opentracing-go"
	tracelog "github.com/opentracing/opentracing-go/log"
	log "github.com/sirupsen/logrus"
)

var (
	ErrNoSuchBooking = errors.New("no such booking")
	hotelsTable      = "hotels"
)

type BookHotelRequest struct {
	Hotel    string    `json:"hotel"`
	CheckIn  time.Time `json:"check_in"`
	CheckOut time.Time `json:"check_out"`
	Name     string    `json:"name"`
	Guests   int       `json:"guests"`
}

func (b *BookHotelRequest) Validate() error {
	if b.Hotel == "" {
		return errors.New("invalid hotel")
	}
	if b.CheckIn.IsZero() {
		return errors.New("invalid check in")
	}
	if b.CheckOut.IsZero() {
		return errors.New("invalid check out")
	}
	if len(b.Name) == 0 {
		return errors.New("invalid name")
	}
	if b.Guests <= 0 {
		return errors.New("invalid number of guests")
	}
	return nil
}

type HotelConfirmation struct {
	Ref   string            `json:"ref"`
	Hotel *BookHotelRequest `json:"hotel"`
}

type HotelService interface {
	BookHotel(context.Context, *BookHotelRequest) (*HotelConfirmation, error)
	GetBooking(ctx context.Context, ref string) (*HotelConfirmation, error)
}

type dynamoService struct {
	db *dynamodb.DynamoDB
}

func NewHotelService() (HotelService, error) {
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
		TableName: aws.String(hotelsTable),
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

	return &dynamoService{db: db}, nil
}

func (d *dynamoService) BookHotel(ctx context.Context, r *BookHotelRequest) (*HotelConfirmation, error) {
	confirmation := &HotelConfirmation{Ref: nuid.Next(), Hotel: r}
	av, err := dynamodbattribute.MarshalMap(confirmation)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(hotelsTable),
	}
	_, err = d.db.PutItemWithContext(ctx, input)

	return confirmation, err
}

func (d *dynamoService) GetBooking(ctx context.Context, ref string) (*HotelConfirmation, error) {
	result, err := d.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(hotelsTable),
		Key: map[string]*dynamodb.AttributeValue{
			"ref": {
				S: aws.String(ref),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	var confirmation *HotelConfirmation
	if err := dynamodbattribute.UnmarshalMap(result.Item, &confirmation); err != nil {
		return nil, err
	}
	if confirmation.Ref == "" {
		return nil, ErrNoSuchBooking
	}

	span, ctx := opentracing.StartSpanFromContext(ctx, "validateReservation")
	span.LogFields(
		tracelog.String("ref", confirmation.Ref),
		tracelog.String("hotel", confirmation.Hotel.Hotel),
		tracelog.String("name", confirmation.Hotel.Name),
	)
	err = d.validateReservation(ctx, confirmation)
	span.Finish()

	return confirmation, err
}

func (d *dynamoService) validateReservation(ctx context.Context, confirmation *HotelConfirmation) error {
	// Do some work.
	n := rand.Intn(4)
	time.Sleep(time.Duration(n) * time.Second)
	log.WithContext(ctx).WithFields(log.Fields{
		"hotel":     confirmation.Hotel.Hotel,
		"check_in":  confirmation.Hotel.CheckIn,
		"check_out": confirmation.Hotel.CheckOut,
		"name":      confirmation.Hotel.Name,
		"guests":    confirmation.Hotel.Guests,
	}).Infof("Validated hotel reservation")
	return nil
}
