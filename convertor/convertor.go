package convertor

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Convertor struct {
	Poller       int
	Worker       int
	SQSName      string
	S3BucketName string
	SQS          *sqs.Client
	SQSUrl       string
}

const (
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
	visibilityTimeout int32 = 30
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html
	waitTimeSeconds int32 = 10 // Long polling
	// Receive message in batch upto 10 (maximum limit)
	maxNumberOfMessages int32 = 10
)

var pollerwg = &sync.WaitGroup{}

func (c *Convertor) Start() {
	// Poller polling for SQS messages
	for w := 1; w < c.Poller; w++ {
		pollerwg.Add(1)
		go c.poller()
		pollerwg.Wait()
	}
}

func (c *Convertor) poller() {
	defer pollerwg.Done()
	for {
		log.Println("Polling for SQS messages...")
		output, err := c.SQS.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl: &c.SQSUrl,
			MessageAttributeNames: []string{
				"All",
			},
			MaxNumberOfMessages: maxNumberOfMessages,
			VisibilityTimeout:   visibilityTimeout,
			WaitTimeSeconds:     waitTimeSeconds,
		})

		if err != nil {
			log.Printf("got an error while receiving messages: %v", err)
			time.Sleep(10. * time.Second)
			continue
		}
		log.Println(output.Messages)
	}
}
