# yql-plus-serverless-example
YQL+ with AWS Serverless example.

## Requirements:
* AWS Account, with a user with enough permissions to do cloudformation creation
* An AWS S3 bucket to store the code jar (for use with `aws cloudformation package`). Default settings are fine.
* Basic java/kotlin development environment set up (java8)

## Build and deploy:
```bash
sed -i '' 's/bucket-name/<your-s3-bucket-here>/' package-deploy.sh
./package-deploy.sh
```

This uses the serverless template in `template.yaml` to create a stack containing a DynamoDB table, and 3 lambda functions with API Gateway triggers.

The following endpoints are created:
* GET `/` - fetches the current stocks
* POST `/` - adds a stock to the portfolio and fetches data for it
* GET `/update/` updates the current prices in the portfolio


## Usage
You can issue the following curl commands to insert data:
```
$ curl -X POST --data "{'stockId': ‘0354.HK', 'quantity': 3000, 'boughtPrice': 34.45}" https://<ref-api-id>.execute-api.ap-southeast-2.amazonaws.com/Prod/
$ curl -X POST --data "{'stockId': '2318.HK', 'quantity': 3000, 'boughtPrice': 34.45}" https://<ref-api-id>.execute-api.ap-southeast-2.amazonaws.com/Prod/
```

Replace `<ref-api-id>` appropriately. Obtain from `AWS console -> API Gateway -> stocks -> Stages`. This will insert the stocks into the db and fetch the current price and name of the stock.

To update and get stock prices:

```
$ curl https://<ref-api-id>.execute-api.ap-southeast-2.amazonaws.com/Prod/update
```

And to simply fetch the current portfolio:
```$ curl https://<ref-api-id>.execute-api.ap-southeast-2.amazonaws.com/Prod/```

When you hit an endpoint for the first time, it will take much longer as the lambda has to start up.

*Everything in here can be done within the AWS free tier.*