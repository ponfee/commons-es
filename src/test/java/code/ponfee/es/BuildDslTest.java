package code.ponfee.es;

import java.util.Arrays;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

public class BuildDslTest {

    private final ElasticSearchClient esClient;

    public BuildDslTest() {
        esClient = new ElasticSearchClient("es-cluster", "127.0.0.1:9300");
    }

    /**
     * search.getClass()                    -> SearchRequestBuilder
     * search.request().getClass()          -> SearchRequest
     * search.request().source().getClass() -> SearchSourceBuilder
     */
    @Test
    public void test1() {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                                              .must(QueryBuilders.existsQuery("aa"))
                                              .mustNot(QueryBuilders.termQuery("bb", ""));
        System.out.println(query.toString());

        SearchRequestBuilder search = esClient.prepareSearch("index", "type")
                                              .setFrom(0).setSize(10).setQuery(query);

        //System.out.println(search.toString()); // search.request().source().toString()
        System.out.println(search.request().toString());
        System.out.println(search.request().source().toString());
        System.out.println(Arrays.toString(search.request().indices()));
        System.out.println(Arrays.toString(search.request().types()));
    }

    @Test
    public void test2() {
        ESQueryBuilder query = ESQueryBuilder.newBuilder("index", "type");
        query.mustExists("aa").mustNotEquals("bb", "");
        query.includeFields("test1", "test2");
        //System.out.println(query.toString(0, 10));
        System.out.println(query.toString());
    }
}
