# Chapter 5

Example code for Chapter 8, section 1

```
$ mvn package 
$ sam deploy --s3-bucket $CF_BUCKET --stack-name chapter8-s3-errors --capabilities CAPABILITY_IAM

$ ERROR_BUCKET="$(aws cloudformation describe-stack-resource --stack-name chapter8-s3-errors --logical-resource-id ErrorTriggeringBucket --query 'StackResourceDetail.PhysicalResourceId' --output text)"
$ aws s3 cp sampledata.json s3://${ERROR_BUCKET}/sampledata.json

$ sam logs -t -n S3ErroringLambda --stack-name chapter8-s3-errors
```

