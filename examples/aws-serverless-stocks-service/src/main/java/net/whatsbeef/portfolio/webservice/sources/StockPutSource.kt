package net.whatsbeef.portfolio.webservice.sources

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper
import com.yahoo.yqlplus.api.Source
import com.yahoo.yqlplus.api.annotations.Query
import net.whatsbeef.portfolio.webservice.model.Stock
import java.util.*

class StockPutSource : Source {
    @Query
    fun putStock(stockId: String, boughtPrice: Double, quantity: Long) : Stock {
        val dynamoDb = getDynamoDb()
        val stock = Stock(stockId, "", boughtPrice, 0.0, Date().time, quantity)
        val mapper = DynamoDBMapper(dynamoDb)
        mapper.save(stock)
        return stock
    }
}