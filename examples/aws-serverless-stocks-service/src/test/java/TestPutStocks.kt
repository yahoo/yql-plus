import com.amazonaws.services.lambda.runtime.Context
import net.whatsbeef.portfolio.webservice.function.GetStock
import net.whatsbeef.portfolio.webservice.function.PutStock
import net.whatsbeef.portfolio.webservice.model.ServerlessInput
import net.whatsbeef.portfolio.webservice.model.ServerlessOutput
import org.junit.Test
import org.mockito.Mockito

class TestPutStocks {

    @Test
    fun testGetStocks() {
        val input = ServerlessInput()
        val context = Mockito.mock(Context::class.java)
        input.body = "{'stockId': ‘0354.HK', 'quantity': 3000, 'boughtPrice': 34.45}"
        PutStock().handleRequest(input, context)
    }
}