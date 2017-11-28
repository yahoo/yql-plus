import com.amazonaws.services.lambda.runtime.Context
import net.whatsbeef.portfolio.webservice.function.GetStock
import net.whatsbeef.portfolio.webservice.function.UpdateStocks
import net.whatsbeef.portfolio.webservice.model.ServerlessInput
import net.whatsbeef.portfolio.webservice.model.ServerlessOutput
import org.junit.Test
import org.mockito.Mockito

class TestGetStocks {

    @Test
    fun testGetStocks() {
        val input = ServerlessInput()
        val context = Mockito.mock(Context::class.java)
        GetStock().handleRequest(input, context)
    }

    @Test
    fun testUpdateStocks() {
        val input = ServerlessInput()
        val context = Mockito.mock(Context::class.java)
        UpdateStocks().handleRequest(input, context)
    }
}