package net.whatsbeef.portfolio.webservice.sources

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

fun getDynamoDb(): AmazonDynamoDB {
    return when (System.getenv("AWS_SAM_LOCAL") != null) {
        true -> AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
                AwsClientBuilder.EndpointConfiguration("http://docker.for.mac.localhost:8000/", "ap-southeast-2")).build()
        false -> AmazonDynamoDBClientBuilder.standard().build()
    }
}