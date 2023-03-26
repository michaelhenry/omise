package omise

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Model interface{}

type Store struct {
	Config aws.Config
	TableName string
}

// Create new store
func NewStore(TableName string) *Store {
	cfg, err := config.LoadDefaultConfig(context.TODO(), func(o *config.LoadOptions) error {
		o.Region = os.Getenv("AWS_DEFAULT_REGION")
		return nil
	})
	if err != nil {
		panic(err)
	}
	return &Store{cfg, TableName}
}

// Save an item
func (s *Store) Save(model Model) (Model, error) {
	av, err := attributevalue.MarshalMap(model)
	if err != nil {
		return nil, err
	}

	client := dynamodb.NewFromConfig(s.Config)
	_, err = client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(s.TableName),
		Item:      av,
	})

	if err != nil {
		return nil, err
	}
	return model, nil
}

// Get a list of items
func (s *Store) Get() ([]Model, error) {
	client := dynamodb.NewFromConfig(s.Config)
	results, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String(s.TableName),
	})
	if err != nil {
		return nil, err
	}

	var records []Model
	err = attributevalue.UnmarshalListOfMaps(results.Items, &records)
	if err != nil {
		return nil, err
	}
	return records, nil
}
