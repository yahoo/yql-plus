import com.google.common.collect.ImmutableMap;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLPlusEngine;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.integration.Movie;
import com.yahoo.yqlplus.integration.MovieSource;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

@Test
public class MovieTest {
    public YQLPlusCompiler createCompiler(Object... bindings) {
        return YQLPlusEngine.builder().bind(bindings).build();
    }

    @Test
    public void requireMovieQuery() throws Exception {
        YQLPlusCompiler compiler = createCompiler(
                "movies", new MovieSource(new Movie("1", "joe", "drama", "2017-01-01", 10))
        );
        CompiledProgram program = compiler.compile("SELECT * FROM movies OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.of());
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Movie> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 1);
        Assert.assertEquals(foo.get(0).getId(), "1");
    }
}
