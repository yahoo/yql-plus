package net.whatsbeef.portfolio.webservice.sources

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression
import com.yahoo.yqlplus.api.Source
import com.yahoo.yqlplus.api.annotations.Key
import com.yahoo.yqlplus.api.annotations.Query
import net.whatsbeef.portfolio.webservice.model.Stock

class StockSource : Source {
    @Query
    fun getStocks(): List<Stock> {
        val dynamoDb = getDynamoDb()
        val mapper = DynamoDBMapper(dynamoDb)
        return mapper.scan(Stock::class.java, DynamoDBScanExpression())
    }

    @Query
    fun getStock(@Key("stockId") stockId: String): Stock {
        val dynamoDb = getDynamoDb()
        val mapper = DynamoDBMapper(dynamoDb)
        return mapper.load(Stock::class.java, stockId)
    }

    fun getStock(junk: List<Stock>, @Key("stockId") stockId: String): Stock {
        return getStock(stockId)
    }
}