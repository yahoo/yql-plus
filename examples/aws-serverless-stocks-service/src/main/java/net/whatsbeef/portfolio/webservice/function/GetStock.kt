package net.whatsbeef.portfolio.webservice.function

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import net.whatsbeef.portfolio.webservice.Programs
import net.whatsbeef.portfolio.webservice.model.ServerlessInput
import net.whatsbeef.portfolio.webservice.model.ServerlessOutput

class GetStock : RequestHandler<ServerlessInput, ServerlessOutput> {
    override fun handleRequest(serverlessInput: ServerlessInput, context: Context): ServerlessOutput {
        val output = ServerlessOutput()
        try {
            output.body = Programs.runProgram("getStockPrices", null, "portfolio")
        } catch (e: Exception) {
            output.statusCode = 500
            output.body = e.message
        }
        return output
    }
}