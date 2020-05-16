package main

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	clusterID             = "j-dfdslkj3kl213kj"
	clusterIDDoesNotExist = "Lorem Ipsum is simply dummy text"
	expectedClusterStatus = "STARTING"
)

var (
	validCluster        = ClusterInput{ClusterID: clusterID}
	emptyCluster        = ClusterInput{ClusterID: ""}
	clusterDoesNotExist = ClusterInput{ClusterID: clusterIDDoesNotExist}
)

// mockEMR represents mock implementation of AWS EMR service
type mockEMR struct {
	emriface.EMRAPI
	mock.Mock
}

// DescribeCluster is a mocked method which return the cluster status
func (m *mockEMR) DescribeCluster(input *emr.DescribeClusterInput) (*emr.DescribeClusterOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*emr.DescribeClusterOutput), args.Error(1)
}

func setup() (*mockEMR, *awsService) {
	mockEMRClient := new(mockEMR)
	mockEMR := &awsService{
		emr: mockEMRClient,
	}

	return mockEMRClient, mockEMR
}

func TestNewAWSService(t *testing.T) {
	awsService := newAWSService()
	assert.NotNil(t, awsService.emr)
}

func TestGetClusterStatus(t *testing.T) {
	testCases := []struct {
		message               string
		clusterID             string
		expectedInput         ClusterInput
		expectedClusterStatus string
		emrError              error
		expectedError         error
	}{
		{
			message:               "When cluster ID is empty, return error",
			clusterID:             "",
			expectedInput:         emptyCluster,
			expectedClusterStatus: "",
			emrError:              nil,
			expectedError:         errors.New("Cluster ID string is empty"),
		},
		{
			message:               "When cluster ID is valid, return status",
			clusterID:             clusterID,
			expectedInput:         validCluster,
			expectedClusterStatus: expectedClusterStatus,
			emrError:              nil,
			expectedError:         nil,
		},
		{
			message:               "when DescribeCluster method fails, return error",
			clusterID:             clusterID,
			expectedInput:         validCluster,
			expectedClusterStatus: "",
			emrError:              errors.New("DescribeCluster method failure"),
			expectedError:         errors.New("DescribeCluster method failure"),
		},
		{
			message:               "when cluster ID does not exist",
			clusterID:             clusterIDDoesNotExist,
			expectedInput:         clusterDoesNotExist,
			expectedClusterStatus: "",
			emrError:              errors.New("cluster ID does not exist"),
			expectedError:         errors.New("cluster ID does not exist"),
		},
	}

	for _, testCase := range testCases {
		mockEMRClient, mockEMR := setup()

		mockDescribeClusterInput := &emr.DescribeClusterInput{
			ClusterId: aws.String(testCase.clusterID),
		}

		mockDescribeClusterOutput := &emr.DescribeClusterOutput{
			Cluster: &emr.Cluster{
				Status: &emr.ClusterStatus{
					State: aws.String(testCase.expectedClusterStatus),
				},
			},
		}

		mockEMRClient.On("DescribeCluster", mockDescribeClusterInput).Return(mockDescribeClusterOutput, testCase.emrError)
		res, err := mockEMR.getClusterStatus(testCase.expectedInput)

		assert.Equal(t, testCase.expectedClusterStatus, res, testCase.message)
		assert.IsType(t, testCase.expectedError, err, testCase.message)
	}
}
