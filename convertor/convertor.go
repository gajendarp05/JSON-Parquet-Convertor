package convertor

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	parquet_s3 "github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type Convertor struct {
	Poller       int
	Worker       int
	SQSName      string
	S3BucketName string
	SQS          *sqs.Client
	SQSUrl       string
	workQueue    chan *types.Message
	Downloader   *manager.Downloader
}

// SQS message body structure
type SqsBody struct {
	Records []struct {
		S3 struct {
			Object struct {
				Key  string // Filename with complete path
				Size int    // Size of file in bytes
			}
		}
	}
}

const (
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
	visibilityTimeout int32 = 30
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html
	waitTimeSeconds int32 = 10 // Long polling
	// Receive message in batch upto 10 (maximum limit)
	maxNumberOfMessages int32 = 10
	parquetExt                = ".parquet"
)

var pollerwg = &sync.WaitGroup{}
var workerwg = &sync.WaitGroup{}

func (c *Convertor) Start() {
	c.workQueue = make(chan *types.Message, c.Worker)
	// Workers for processing SQS messages
	for w := 1; w < c.Worker; w++ {
		workerwg.Add(1)
		go c.worker()
	}

	// Poller polling for SQS messages
	for w := 1; w < c.Poller; w++ {
		pollerwg.Add(1)
		go c.poller()
	}

	workerwg.Done()
	pollerwg.Wait()
}

// poller polls continuously to check and retrieves new messages from SQS queue
// and send them on worker channel for processing
func (c *Convertor) poller() {
	defer pollerwg.Done()
	for {
		log.Println("Polling for messages from SQS queue...")
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
			// Sleeping for some x time before start polling again
			time.Sleep(10. * time.Second)
			continue
		}
		// writing SQS message to the worker channel for processing
		for _, m := range output.Messages {
			c.workQueue <- &m
		}
	}
}

// worker start by downloading the message, converting the file from JSON to Parquet and then cleaning up the SQS message
func (c *Convertor) worker() {
	defer workerwg.Done()
	for m := range c.workQueue {
		var sqsobj SqsBody
		err := json.Unmarshal([]byte(*m.Body), &sqsobj)
		if err != nil {
			log.Println("error while marshaling messages: ", err)
			continue
		}
		// key is path of JSON file for download (prefix + filename)
		key, err := url.QueryUnescape(sqsobj.Records[0].S3.Object.Key)
		if err != nil {
			log.Print("unescaping of key "+key+" failed with err: ", err)
			continue
		}
		buf := make([]byte, int(sqsobj.Records[0].S3.Object.Size))
		w := manager.NewWriteAtBuffer(buf)

		// download JSON file from S3 bucket
		_, err = c.Downloader.Download(context.TODO(), w, &s3.GetObjectInput{
			Bucket: aws.String(c.S3BucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			log.Println("error during downloading from s3: ", err)
			continue
		}

		pw, fw := c.parquetFileWriter(key)

		var item personJson
		if err := json.Unmarshal(w.Bytes(), &item); err != nil {
			log.Println("failed to unmarshal ", err)
			continue
		}
		parquetObj := toParquet(item)
		if err := pw.Write(parquetObj); err != nil {
			log.Println("error in writing into parquet file")
			continue
		}

		if err = pw.WriteStop(); err != nil {
			log.Print("writeStop error", err)
			continue
		}

		fw.Close()
		log.Printf("JSON file %s to Parquet conversion is successful: ", key)

		// delete the message from queue after successful JSON -> Parquet conversion
		_, err = c.SQS.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
			QueueUrl:      &c.SQSUrl,
			ReceiptHandle: m.ReceiptHandle,
		})
		if err != nil {
			log.Print("got an error deleting the message from SQS: ", err)
			continue
		}
	}
}

// parquetFileWriter writer returns parquet handler and S3 file writer
func (c *Convertor) parquetFileWriter(key string) (*writer.ParquetWriter, source.ParquetFile) {
	ctx := context.Background()
	fw, err := parquet_s3.NewS3FileWriter(ctx, c.S3BucketName, key+parquetExt, nil)
	if err != nil {
		log.Fatalln("error in creating local file writer: ", err)
	}
	pw, err := writer.NewParquetWriter(fw, new(personParquet), 1)
	if err != nil {
		log.Fatalln("error in creating parquet file writer: ", err)
	}
	// http://cloudsqale.com/2020/05/29/how-parquet-files-are-written-row-groups-pages-required-memory-and-flush-operations/
	pw.RowGroupSize = 128 * 128 * 1024
	// https://github.com/xitongsys/parquet-go#compression-type
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	return pw, fw
}
