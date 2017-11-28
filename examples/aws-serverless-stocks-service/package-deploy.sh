#!/bin/bash
S3_BUCKET="bucket-name"

./gradlew shadowJar
aws cloudformation package --template-file ./template.yaml --s3-bucket $S3_BUCKET --output-template-file packaged-template.yaml
aws cloudformation deploy --template-file ./packaged-template.yaml --stack-name stocks --capabilities CAPABILITY_IAM
