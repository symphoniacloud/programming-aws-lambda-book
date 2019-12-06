# Chapter 3

Example from Chapter 4 - this builds a Lambda artifact in a zip-file format, and also
uses the reproducible build Maven plugin.

## Prerequisites

* Created: An AWS account, and an S3 bucket for storing temporary deployment artifacts (referred to as $CF_BUCKET below)
* Installed: AWS CLI, Maven, SAM CLI
* Configured: AWS credentials in your terminal

## Usage

To build:

```
$ mvn package
```

To deploy:

```
$ sam deploy --s3-bucket $CF_BUCKET --stack-name ChapterFour --capabilities CAPABILITY_IAM
```

To test:

The above will create a new function in Lambda, so you can test via the Lambda web console,
or via the CLI using `aws lambda invoke`.

