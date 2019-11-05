package code.ponfee.es;

import java.util.Arrays;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.nlpcn.es4sql.query.ESActionFactory;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.SqlElasticSearchRequestBuilder;

public class SqlToDslTest {

    @Test
    public void test1() throws Exception {
        String sql = "SELECT case when gender is null then 'aaa'  else gender  end  test , cust_code FROM elasticsearch-sql_test_index";
        String dsl = ESActionFactory.create(SqlParserClient.get(), sql).explain().explain();
        System.out.println(dsl);
    }

    @Test
    public void test2() throws Exception {
        String sql = "SELECT COUNT(*) AS mycount FROM elasticsearch-sql_test_index/account";
        SqlElasticRequestBuilder srb = ESActionFactory.create(SqlParserClient.get(), sql).explain();
        System.out.println(srb.explain());
    }

    /**
     * action.explain().getClass()                          -> SqlElasticSearchRequestBuilder
     * sqlSearch.getBuilder().getClass()                    -> SearchRequestBuilder
     * sqlSearch.getBuilder().request().getClass()          -> SearchRequest
     * sqlSearch.getBuilder().request().source().getClass() -> SearchSourceBuilder
     * @throws Exception
     */
    @Test
    public void test3() throws Exception {
        String sql = "SELECT case when gender is null then 'aaa'  else gender  end  test , cust_code FROM elasticsearch-sql_test_index";
        SqlElasticSearchRequestBuilder sqlSearch = (SqlElasticSearchRequestBuilder) ESActionFactory.create(SqlParserClient.get(), sql).explain();
        //sqlSearch.explain(); // SearchRequestBuilder.toString()
        SearchRequestBuilder search = (SearchRequestBuilder) sqlSearch.getBuilder();

        SearchRequest request = search.request();
        System.out.println(Arrays.toString(request.indices()));
        System.out.println(Arrays.toString(request.types()));
        SearchSourceBuilder source = request.source();
        System.out.println(source.toString());
    }

    @Test
    public void test4() throws Exception {
        String sql = "DELETE FROM test_index where id=1";
        System.out.println(SqlParserClient.get().parseSql(sql));
    }

    @Test
    public void test5() throws Exception {
        String sql = "SELECT skuNo, max(itemClass) t FROM test_index group by skuNo ";
        System.out.println(SqlParserClient.get().parseSql(sql));
    }
}
