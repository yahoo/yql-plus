package net.whatsbeef.portfolio.webservice.function

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.google.gson.Gson
import net.whatsbeef.portfolio.webservice.Programs
import net.whatsbeef.portfolio.webservice.model.ServerlessInput
import net.whatsbeef.portfolio.webservice.model.ServerlessOutput
import net.whatsbeef.portfolio.webservice.model.Stock
import java.util.*

class PutStock : RequestHandler<ServerlessInput, ServerlessOutput> {
    override fun handleRequest(serverlessInput: ServerlessInput, context: Context): ServerlessOutput {
        val output = ServerlessOutput()
        val stock: Stock = Gson().fromJson(serverlessInput.body, Stock::class.java)
        val params: HashMap<String, Any> = hashMapOf(
            "stockId" to stock.stockId,
                "boughtPrice" to stock.boughtPrice,
                "quantity" to stock.quantity
        )
        try {
            output.statusCode = 200
            output.body = Programs.runProgram("putStock", params, "result")
        } catch (e: Exception) {
            output.statusCode = 500
            output.body = e.message
        }
        return output
    }
}