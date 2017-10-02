package net.whatsbeef.portfolio.webservice.sources

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper
import com.github.kittinunf.fuel.httpGet
import com.jayway.jsonpath.JsonPath
import com.yahoo.yqlplus.api.Source
import com.yahoo.yqlplus.api.annotations.Key
import com.yahoo.yqlplus.api.annotations.Query
import net.whatsbeef.portfolio.webservice.model.Stock

class StockUpdateSource: Source {
    @Query
    fun updateStocks(stocks: List<Stock>): List<Stock> {
        //https://query.yahooapis.com/v1/public/yql?q=use%20%22https%3A%2F%2Fraw.githubusercontent.com%2Fyql%2Fyql-tables%2Fmaster%2Fyahoo%2Ffinance%2Fyahoo.finance.quotes.xml%22%20as%20quotes%3B%0Aselect%20symbol%2C%20Name%2C%20LastTradePriceOnly%20from%20quotes%20where%20symbol%20in%20(%220354.HK%22%2C%20%222318.HK%22%2C%20%221800.HK%22)&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=
        val dynamoDb = getDynamoDb()
        val mapper = DynamoDBMapper(dynamoDb)
        val stockCSV = stocks.map { it.stockId }.joinToString("\",\"", "\"", "\"")
        val (_, _, result) = "https://query.yahooapis.com/v1/public/yql".httpGet(listOf(
                "q" to "use \"https://raw.githubusercontent.com/yql/yql-tables/master/yahoo/finance/yahoo.finance.quotes.xml\" as quotes;\n" +
                        "select symbol, Name, LastTradePriceOnly from quotes where symbol in ($stockCSV)",
                "format" to "json",
                "callback" to "")).responseString()
        val quotes: List<Map<String, String>> = when (stocks.size > 1) {
            true -> JsonPath.parse(result.get()).read("$.query.results.quote")
            false -> listOf(JsonPath.parse(result.get()).read("$.query.results.quote"))
        }
        var updatedStocks = ArrayList<Stock>()
        for ((index, quote) in quotes.withIndex()) {
            var stock = stocks[index]
            val price = quote["LastTradePriceOnly"]?.toDouble()
            val name = quote["Name"]
            price?.let {
                stock.currentPrice = price
                name?.let { stock.stockName = name }
                mapper.save(stock)
                updatedStocks.add(stock)
            }
        }
        return updatedStocks
    }

    @Query
    fun updateStock(@Key("stockId") stockId: String): Stock {
        val dynamoDb = getDynamoDb()
        val mapper = DynamoDBMapper(dynamoDb)
        val stock = mapper.load(Stock::class.java, stockId)
        return updateStocks(listOf(stock))[0]
    }
}
