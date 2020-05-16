package main

import (
	"github.com/aws/aws-lambda-go/lambda"
)

func main()  {
	handler := newAWSService()

	lambda.Start(handler.getClusterStatus)
}