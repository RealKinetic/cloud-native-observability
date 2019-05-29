package service

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/nats-io/nuid"
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
	BookHotel(*BookHotelRequest) (*HotelConfirmation, error)
	GetBooking(ref string) (*HotelConfirmation, error)
}

type dynamoService struct {
	db *dynamodb.DynamoDB
}

func NewHotelService() (HotelService, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String("us-east-1")},
	}))
	db := dynamodb.New(sess)

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

func (d *dynamoService) BookHotel(r *BookHotelRequest) (*HotelConfirmation, error) {
	confirmation := &HotelConfirmation{Ref: nuid.Next(), Hotel: r}
	av, err := dynamodbattribute.MarshalMap(confirmation)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(hotelsTable),
	}
	_, err = d.db.PutItem(input)

	return confirmation, err
}

func (d *dynamoService) GetBooking(ref string) (*HotelConfirmation, error) {
	result, err := d.db.GetItem(&dynamodb.GetItemInput{
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

	var r *HotelConfirmation
	if err := dynamodbattribute.UnmarshalMap(result.Item, &r); err != nil {
		return nil, err
	}
	if r.Ref == "" {
		return nil, ErrNoSuchBooking
	}
	return r, nil
}
