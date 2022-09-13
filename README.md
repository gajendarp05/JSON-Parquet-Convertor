The JSON to Parquet convertor service reads JSON file from S3 bucket and convert it into Parquet file format and write it back to S3.

<<<<<<< HEAD
We are using Golang AWS SDK v2 for all the interaction with AWS infrastructure.
To read more about v2 - https://aws.github.io/aws-sdk-go-v2/docs/getting-started/

Following AWS services are used:
1. S3 - for storing json and parquet files
2. SQS queue - notifying whenever new json files is uploaded to s3 bucket
=======
We will be using Golang AWS SDK v2 for all the interaction with AWS infrastructure

To read more about v2 - https://aws.github.io/aws-sdk-go-v2/docs/getting-started/
>>>>>>> 7558c8bf15285b217ff312cba1ef3a53b0f28109
