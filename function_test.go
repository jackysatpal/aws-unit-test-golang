package main

import (
	"errors"
	"testing"
	"time"

	"github.cerner.com/healtheintent/realworlddata/src/entities"
	apperrors "github.cerner.com/healtheintent/realworlddata/src/errors"
	"github.cerner.com/healtheintent/realworlddata/src/testutils/updateclustertestutils"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/emr"

	"github.com/stretchr/testify/assert"
)

var (
	emptyClusterID   = ""
	validclusterID   = "j-3HF05G4MQF2LM"
	invalidClusterID = "realworlddata-test"
)

var (
	emptyClusterStatus = ""
	validClusterStatus = "TERMINATED"
)

var (
	emptyCluster   = entities.Cluster{ClusterID: emptyClusterID}
	validCluster   = entities.Cluster{ClusterID: validclusterID}
	invalidCluster = entities.Cluster{ClusterID: invalidClusterID}
)

var (
	emptyMessage   = ""
	successMessage = "Updated cluster status successfully"
)

var currentTime = time.Now().Format(time.RFC3339)
var mockTableName = "test"

func setup() (*updateclustertestutils.MockUpdateCluster, *UpdateCluster) {
	mockClient := new(updateclustertestutils.MockUpdateCluster)
	mockServices := &UpdateCluster{
		dbIface:   mockClient,
		emrIface:  mockClient,
		tableName: mockTableName,
	}
	return mockClient, mockServices
}

func TestNewUpdateCluster(t *testing.T) {
	updateCluster := NewUpdateCluster("test")
	assert.NotNil(t, updateCluster.dbIface)
	assert.NotNil(t, updateCluster.emrIface)
	assert.Equal(t, mockTableName, updateCluster.tableName)
}

func TestUpdateClusterStatus(t *testing.T) {
	testCases := []struct {
		message       string
		clusterID     string
		clusterStatus string
		updatedAt     string
		entities.Cluster
		expectedMessage string
		emrError        error
		dynamodbError   error
		expectedError   error
	}{
		{
			"when clusterID is empty, return empty string for message and error message",
			emptyClusterID,
			emptyClusterStatus,
			currentTime,
			emptyCluster,
			emptyMessage,
			nil,
			nil,
			apperrors.NewInvalidInputError("clusterID is empty"),
		},
		{
			"when clusterID is valid, return success message and nil for error",
			validclusterID,
			validClusterStatus,
			currentTime,
			validCluster,
			successMessage,
			nil,
			nil,
			nil,
		},
		{
			"when DescribeCluster method fails, return empty string for message and error message",
			validclusterID,
			emptyClusterStatus,
			currentTime,
			validCluster,
			emptyMessage,
			errors.New("DescribeCluster method failure"),
			nil,
			errors.New("DescribeCluster method failure"),
		},
		{
			"when UpdateItem method fails, return empty string for message and error message",
			validclusterID,
			emptyClusterStatus,
			currentTime,
			validCluster,
			emptyMessage,
			nil,
			errors.New("UpdateItem method failure"),
			errors.New("UpdateItem method failure"),
		},
		{
			"when clusterID is invalid, return empty string for message and nil for error",
			invalidClusterID,
			emptyClusterStatus,
			currentTime,
			invalidCluster,
			emptyMessage,
			apperrors.NewNotFoundError("clusterID does not exist"),
			nil,
			apperrors.NewNotFoundError("clusterID does not exist"),
		},
	}

	for _, testCase := range testCases {
		mockClient, mockServices := setup()

		mockDescribeClusterInput := &emr.DescribeClusterInput{
			ClusterId: aws.String(testCase.clusterID),
		}
		mockDescribeClusterOutput := &emr.DescribeClusterOutput{
			Cluster: &emr.Cluster{
				Status: &emr.ClusterStatus{
					State: aws.String(testCase.clusterStatus),
				},
			},
		}

		mockUpdateItemInput := &dynamodb.UpdateItemInput{
			TableName: aws.String(mockTableName),
			Key: map[string]*dynamodb.AttributeValue{
				entities.ClusterID: {S: aws.String(testCase.clusterID)},
			},
			UpdateExpression: aws.String("SET #S = :s, #U = :u"),
			ExpressionAttributeNames: map[string]*string{
				"#S": aws.String(entities.ClusterStatus),
				"#U": aws.String(entities.ClusterUpdatedAt),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":s": {
					S: aws.String(testCase.clusterStatus),
				},
				":u": {
					S: aws.String(testCase.updatedAt),
				},
			},
			ReturnValues: aws.String("UPDATED_NEW"),
		}
		mockUpdateItemOutput := &dynamodb.UpdateItemOutput{
			Attributes: map[string]*dynamodb.AttributeValue{
				"S": {
					S: aws.String(testCase.clusterStatus),
				},
			},
		}

		mockClient.On("DescribeCluster", mockDescribeClusterInput).Return(mockDescribeClusterOutput, testCase.emrError)
		mockClient.On("UpdateItem", mockUpdateItemInput).Return(mockUpdateItemOutput, testCase.dynamodbError)

		res, err := mockServices.UpdateClusterStatus(testCase.Cluster)

		assert.Equal(t, testCase.expectedMessage, res, testCase.message)
		assert.IsType(t, testCase.expectedError, err, testCase.message)
	}
}
