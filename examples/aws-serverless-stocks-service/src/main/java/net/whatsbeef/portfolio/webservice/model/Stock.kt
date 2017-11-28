package net.whatsbeef.portfolio.webservice.model

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable

@DynamoDBTable(tableName = "stocks")
data class Stock(@DynamoDBHashKey(attributeName = "stockId") var stockId: String,
                 @DynamoDBAttribute(attributeName = "stockName") var stockName: String,
                 @DynamoDBAttribute(attributeName = "boughtPrice") var boughtPrice: Double,
                 @DynamoDBAttribute(attributeName = "currentPrice") var currentPrice: Double,
                 @DynamoDBAttribute(attributeName = "boughtDate") var boughtDate: Long,
                 @DynamoDBAttribute(attributeName = "quantity") var quantity: Long) {

    constructor() : this("", "", 0.0 , 0.0, 0, 0)
}