package main

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"

	log "github.com/sirupsen/logrus"
)

// ClusterInput represent input which will be given to the lambda
type ClusterInput struct {
	ClusterID string `json:"clusterID"`
}

// awsService represents emr interface
type awsService struct {
	emr emriface.EMRAPI
}

// newAWSService returns a new instance of emr
func newAWSService() *awsService {
	awsConfig := &aws.Config{Region: aws.String("us-west-2")}
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		log.Errorf("error while creating AWS session - %s", err.Error())
	}

	return &awsService{
		emr: emr.New(sess),
	}
}

// getClusterStatus returns current cluster status along with an error
func (svc *awsService) getClusterStatus(input ClusterInput) (string, error) {
	clusterID := input.ClusterID
	if clusterID == "" {
		return "", errors.New("clusterID is empty")
	}

	describeClusterInput := &emr.DescribeClusterInput{
		ClusterId: aws.String(clusterID),
	}

	clusterDetails, err := svc.emr.DescribeCluster(describeClusterInput)
	if err != nil {
		log.Errorf("DescribeCluster error - %s", err)
		return "", err
	}

	if clusterDetails == nil {
		log.Errorf("clusterID does not exist")
		return "", errors.New("clusterID does not exist")
	}

	clusterStatus := *clusterDetails.Cluster.Status.State

	return string(clusterStatus), nil
}
