The JSON to Parquet convertor service reads JSON file from Amazon S3 bucket and convert it into Parquet file format and write it back to Amazon S3.

We are using Golang AWS SDK v2 for all the interaction with AWS infrastructure.
To read more about v2 - https://aws.github.io/aws-sdk-go-v2/docs/getting-started/

Following Amazon web services are used:
1. Amazon S3 - for storing JSON and Apache Parquet files
2. SQS Queue - notifying whenever new JSON files is uploaded/ created to S3 bucket.

For more details, checkout this blog: https://gajendarp05.medium.com/json-to-apache-parquet-convertor-service-in-golang-7eedb91ca8f8
