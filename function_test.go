package main

import (
	"errors"
	"testing"

	"github.cerner.com/healtheintent/realworlddata/src/entities"
	apperrors "github.cerner.com/healtheintent/realworlddata/src/errors"
	"github.cerner.com/healtheintent/realworlddata/src/testutils/emrtestutils"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/stretchr/testify/assert"
)

package emrtestutils

import (
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"

	"github.com/stretchr/testify/mock"
)

// MockEMR represents a mocked impl of EMRAPI which
// can be used to test our methods that rely on emr
type MockEMR struct {
	emriface.EMRAPI
	mock.Mock
}

// DescribeCluster is a mocked method which return the cluster status
func (m *MockEMR) DescribeCluster(input *emr.DescribeClusterInput) (*emr.DescribeClusterOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*emr.DescribeClusterOutput), args.Error(1)
}

// RunJobFlow is a mocked method which returns a run job flow response
func (m *MockEMR) RunJobFlow(input *emr.RunJobFlowInput) (*emr.RunJobFlowOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*emr.RunJobFlowOutput), args.Error(1)
}

// AddJobFlowSteps is a mocked method which returns an add job flow steps response
func (m *MockEMR) AddJobFlowSteps(input *emr.AddJobFlowStepsInput) (*emr.AddJobFlowStepsOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*emr.AddJobFlowStepsOutput), args.Error(1)
}


var (
	clusterID             = "j-dfdslkj3kl213kj"
	clusterIDDoesNotExist = "Lorem Ipsum is simply dummy text"
	expectedClusterStatus = "STARTING"
)

var (
	validCluster        = entities.Cluster{ClusterID: clusterID}
	emptyCluster        = entities.Cluster{ClusterID: ""}
	clusterDoesNotExist = entities.Cluster{ClusterID: clusterIDDoesNotExist}
)

func setup() (*emrtestutils.MockEMR, *EMR) {
	mockEMRClient := new(emrtestutils.MockEMR)
	mockEMR := &EMR{
		Client: mockEMRClient,
	}

	return mockEMRClient, mockEMR
}

func TestNewEMR(t *testing.T) {
	emr := NewEMR()
	assert.NotNil(t, emr.Client)
}

func TestGetClusterStatus(t *testing.T) {
	testCases := []struct {
		message               string
		clusterID             string
		input                 entities.Cluster
		expectedClusterStatus string
		emrError              error
		expectedError         error
	}{
		{
			"When cluster ID is empty, return error",
			"",
			emptyCluster,
			"",
			nil,
			apperrors.NewInvalidInputError("Cluster ID string is empty"),
		},
		{
			"When cluster ID is valid, return status",
			clusterID,
			validCluster,
			expectedClusterStatus,
			nil,
			nil,
		},
		{
			"when DescribeCluster method fails, return error",
			clusterID,
			validCluster,
			"",
			errors.New("DescribeCluster method failure"),
			errors.New("DescribeCluster method failure"),
		},
		{
			"when cluster ID does not exist",
			clusterIDDoesNotExist,
			clusterDoesNotExist,
			"",
			apperrors.NewNotFoundError("cluster ID does not exist"),
			apperrors.NewNotFoundError("cluster ID does not exist"),
		},
	}

	for _, testCase := range testCases {
		mockEMRClient, mockEMR := setup()

		input := &emr.DescribeClusterInput{ClusterId: aws.String(testCase.clusterID)}
		output := &emr.DescribeClusterOutput{
			Cluster: &emr.Cluster{
				Status: &emr.ClusterStatus{
					State: aws.String(testCase.expectedClusterStatus),
				},
			},
		}

		mockEMRClient.On("DescribeCluster", input).Return(output, testCase.emrError)
		res, err := mockEMR.GetClusterStatus(testCase.input)

		assert.Equal(t, testCase.expectedClusterStatus, res, testCase.message)
		assert.IsType(t, testCase.expectedError, err, testCase.message)
	}
}
