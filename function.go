// Package main runs a lambda which will get the current cluster status and update that status in a dynamodb table
package main

import (
	"fmt"
	"time"

	"github.cerner.com/healtheintent/realworlddata/src/entities"
	"github.cerner.com/healtheintent/realworlddata/src/errors"
	"github.cerner.com/healtheintent/realworlddata/src/sessions"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"

	log "github.com/sirupsen/logrus"
)

const (
	// ClusterID is the primary key for the specified table in the serverless.yml
	ClusterID = "clusterID"
	// ClusterStatus is a column name for the specified table in the serverless.yml
	ClusterStatus = "status"
	// ClusterUpdatedAt is a column name for the specified table in the serverless.yml
	ClusterUpdatedAt = "updatedAt"
)

// Cluster is a list of attributes associated with an AWS EMR Cluster
type Cluster struct {
	ClusterID string `json:"clusterID"`
	Tenant    string `json:"tenant"`
}

// UpdateCluster uses the iface of the emr and dynamodb service which are required for the unit testing
// it also includes dynamodb table name
type UpdateCluster struct {
	emrIface  emriface.EMRAPI
	dbIface   dynamodbiface.DynamoDBAPI
	tableName string
}

// NewUpdateCluster returns a new instance of the dynamodb, emr service and specifies the table name
func NewUpdateCluster(tableName string) *UpdateCluster {
	sess := sessions.NewSession()
	return &UpdateCluster{
		dbIface:   dynamodb.New(sess),
		emrIface:  emr.New(sess),
		tableName: tableName,
	}
}

// UpdateClusterStatus is a lambda which will get the current cluster status based on clusterID
// and update that status in a dynamodb table
//
// entities.Cluster.ClusterID       - is a primary key that is being used to get an item and update that particular item
// entities.Cluster.ClusterStatus   - is a column in a dynamodb table, it represents the current cluster status
// entities.Cluster.ClusterUpdateAt - is a column in a dynamodb table, it represents when the status was updated
//
// return a success/fail string along with an error
func (svc *UpdateCluster) UpdateClusterStatus(e entities.Cluster) (string, error) {
	clusterID := e.ClusterID
	if clusterID == "" {
		return "", errors.NewInvalidInputError("clusterID is empty")
	}

	describeClusterInput := &emr.DescribeClusterInput{ClusterId: aws.String(clusterID)}
	clusterDetails, err := svc.emrIface.DescribeCluster(describeClusterInput)
	if err != nil {
		log.Errorf("functions.updateClusterStatus: DescribeCluster error - %s", err)
		return "", err
	}
	if clusterDetails == nil {
		log.Errorf("functions.updateClusterStatus: clusterID does not exist")
		return "", errors.NewNotFoundError("clusterID does not exist")
	}

	clusterStatus := *clusterDetails.Cluster.Status.State
	// time in ISO 8601 format
	updatedAt := time.Now().Format(time.RFC3339)

	updateItemInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(svc.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			entities.ClusterID: {S: aws.String(clusterID)},
		},
		UpdateExpression: aws.String("SET #S = :s, #U = :u"),
		ExpressionAttributeNames: map[string]*string{
			"#S": aws.String(entities.ClusterStatus),
			"#U": aws.String(entities.ClusterUpdatedAt),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":s": {
				S: aws.String(clusterStatus),
			},
			":u": {
				S: aws.String(updatedAt),
			},
		},
		ReturnValues: aws.String("UPDATED_NEW"),
	}

	res, err := svc.dbIface.UpdateItem(updateItemInput)
	if err != nil {
		log.Errorf("functions.updateClusterStatus: UpdateItem error - %s", err)
		return "", err
	}

	fmt.Println(res)
	return "Updated cluster status successfully", nil
}
