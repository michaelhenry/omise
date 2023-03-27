package omise

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Model interface {
	Pk() string
	Sk() string
}

type Store struct {
	client    *dynamodb.Client
	tableName string
}

type awsCreds struct {
	keyId string
	secretKey string
}

func (c *awsCreds)	Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID: c.keyId, SecretAccessKey: c.secretKey,
	}, nil
}

func NewStore(TableName string, Region string, KeyId string, SecretKey string) *Store {
	cfg, err := config.LoadDefaultConfig(context.TODO(), func(o *config.LoadOptions) error {
		o.Region = Region
		o.Credentials = &awsCreds{
			keyId: KeyId,
			secretKey: SecretKey,
		}
		return nil
	})

	if err != nil {
		panic(err)
	}

  client := dynamodb.NewFromConfig(cfg)
	return &Store{client, TableName}
}

func (s *Store) saveItem(model Model) (map[string]types.AttributeValue, error) {
	av, err := attributevalue.MarshalMap(model)
	if err != nil {
		return nil, err
	}

	av["Pk"] = &types.AttributeValueMemberS{Value: model.Pk()}
	av["Sk"] = &types.AttributeValueMemberS{Value: model.Sk()}
	out, err := s.client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      av,
	})
	return out.Attributes, err
}

func (s *Store) getItem(pk string, sk string) (map[string]types.AttributeValue, error) {
	out, err := s.client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"Pk": &types.AttributeValueMemberS{Value: pk},
			"Sk": &types.AttributeValueMemberS{Value: sk},
		},
	})
	return out.Item, err
}

func (s *Store) getItems(filterExpression string, expressionAttributeValues map[string]types.AttributeValue) ([]map[string]types.AttributeValue, error) {
	out, err := s.client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName:                 &s.tableName,
		FilterExpression:          aws.String(filterExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		Limit:                     aws.Int32(100),
	})
	return out.Items, err
}

// Transform map to model.
func GetItem[model any](store *Store, pk string, sk string) (*model, error) {
	out, err := store.getItem(pk, sk)
	if err != nil {
		panic(err)
	}
	obj := new(model)
	if err = attributevalue.UnmarshalMap(out, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func GetItems[model any](store *Store, filterExpression string, expressionAttributeValues map[string]types.AttributeValue) ([]model, error) {
	out, err := store.getItems(filterExpression, expressionAttributeValues)
	if err != nil {
		panic(err)
	}
	objs := new([]model)
	if err = attributevalue.UnmarshalListOfMaps(out, &objs); err != nil {
		return nil, err
	}
	return *objs, err
}

func SaveItem(store *Store, model Model) error {
	_, err := store.saveItem(model)
	return err
}
