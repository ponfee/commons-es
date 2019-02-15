package code.ponfee.es;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * 查询条件构建
 * @author fupf
 */
public class ESQueryBuilder {

    private final String[] indices; // 索引
    private final String[] types; // 类型
    private BoolQueryBuilder boolQuery; // bool筛选条件
    private String[] includeFields;
    private String[] excludeFields;

    private final List<SortBuilder<?>> sorts = new ArrayList<>(1); // 排序
    private final List<AggregationBuilder> aggs = new ArrayList<>(1); // 分组聚合

    private ESQueryBuilder(String[] indices, String[] types) {
        this.indices = indices;
        this.types = types;
    }

    public static ESQueryBuilder newBuilder(String index, String... type) {
        return new ESQueryBuilder(new String[] { index }, type);
    }

    public static ESQueryBuilder newBuilder(String[] indices, String... types) {
        return new ESQueryBuilder(indices, types);
    }

    public ESQueryBuilder includeFields(String... includeFields) {
        if (this.includeFields != null) {
            throw new UnsupportedOperationException("Cannot repeat set include fields.");
        }
        this.includeFields = includeFields;
        return this;
    }

    public ESQueryBuilder excludeFields(String... excludeFields) {
        if (this.excludeFields != null) {
            throw new UnsupportedOperationException("Cannot repeat set exclude fields.");
        }
        this.excludeFields = excludeFields;
        return this;
    }

    // -----------The clause (query) must appear in matching documents，所有term全部匹配（AND）--------- //
    /**
     * ?=term
     * @param name
     * @param term
     * @return
     */
    public ESQueryBuilder mustEquals(String name, Object term) {
        query().must(QueryBuilders.termQuery(name, term));
        return this;
    }

    /**
     * ? !=term
     * @param name
     * @param term
     * @return
     */
    public ESQueryBuilder mustNotEquals(String name, Object term) {
        query().mustNot(QueryBuilders.termQuery(name, term));
        return this;
    }

    /**
     * ? IN(item1,item2,..,itemn)
     * @param name
     * @param items  
     * @return
     */
    public <T> ESQueryBuilder mustIn(String name, List<T> items) {
        query().must(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * ? IN(item1,item2,..,itemn)
     * @param name
     * @param items  
     * @return
     */
    public ESQueryBuilder mustIn(String name, Object... items) {
        query().must(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * ? NOT IN(item1,item2,..,itemn)
     * @param name
     * @param items
     * @return
     */
    public ESQueryBuilder mustNotIn(String name, Object... items) {
        query().mustNot(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * must range query
     * ? BETWEEN from AND to
     * @param name
     * @param from
     * @param to
     * @return
     */
    public ESQueryBuilder mustRange(String name, Object from, Object to) {
        query().must(QueryBuilders.rangeQuery(name).from(from).to(to));
        return this;
    }

    /**
     * !(BETWEEN from AND to)
     * @param name
     * @param from
     * @param to
     * @return
     */
    public ESQueryBuilder mustNotRange(String name, Object from, Object to) {
        query().mustNot(QueryBuilders.rangeQuery(name).from(from).to(to));
        return this;
    }

    /**
     * EXISTS(name) && name IS NOT NULL && name IS NOT EMPTY
     * @param name
     * @return
     */
    public ESQueryBuilder mustExists(String name) {
        query().must(QueryBuilders.existsQuery(name));
        return this;
    }

    /**
     * NOT EXISTS(name) && name IS NULL && name IS EMPTY
     * @param name
     * @return
     */
    public ESQueryBuilder mustNotExists(String name) {
        query().mustNot(QueryBuilders.existsQuery(name));
        return this;
    }

    /**
     * name LIKE 'prefix%'
     * @param name
     * @param prefix
     * @return
     */
    public ESQueryBuilder mustPrefix(String name, String prefix) {
        query().must(QueryBuilders.prefixQuery(name, prefix));
        return this;
    }

    /**
     * name NOT LIKE 'prefix%'
     * @param name
     * @param prefix
     * @return
     */
    public ESQueryBuilder mustNotPrefix(String name, String prefix) {
        query().mustNot(QueryBuilders.prefixQuery(name, prefix));
        return this;
    }

    /**
     * regexp query
     * @param name
     * @param regexp
     * @return
     */
    public ESQueryBuilder mustRegexp(String name, String regexp) {
        query().must(QueryBuilders.regexpQuery(name, regexp));
        return this;
    }

    /**
     * regexp query
     * @param name
     * @param regexp
     * @return
     */
    public ESQueryBuilder mustNotRegexp(String name, String regexp) {
        query().mustNot(QueryBuilders.regexpQuery(name, regexp));
        return this;
    }

    /**
     * like query
     * name LIKE '*wildcard*'
     * @param name
     * @param wildcard
     * @return
     */
    public ESQueryBuilder mustLike(String name, String wildcard) {
        query().must(QueryBuilders.wildcardQuery(name, wildcard));
        return this;
    }

    /**
     * not like
     * @param name  before or after or both here with *
     * @param wildcard
     * @return
     */
    public ESQueryBuilder mustNotLike(String name, String wildcard) {
        query().mustNot(QueryBuilders.wildcardQuery(name, wildcard));
        return this;
    }

    // --------------The clause (query) should appear in the matching document. ------------- //
    // --------------In a boolean query with no must clauses, one or more should clauses must match a document. ------------- //
    // --------------The minimum number of should clauses to match can be set using the minimum_should_matchparameter. ------------- //
    // --------------至少有一个term匹配（OR） ------------- //
    /**
     * ?=text
     * @param name
     * @param term 
     * @return
     */
    public ESQueryBuilder shouldEquals(String name, Object term) {
        query().should(QueryBuilders.termQuery(name, term));
        return this;
    }

    /**
     * ? IN(item1,item2,..,itemn)
     * @param name
     * @param items
     * @return
     */
    public <T> ESQueryBuilder shouldIn(String name, List<T> items) {
        query().should(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * ? IN(item1,item2,..,itemn)
     * @param name
     * @param items
     * @return
     */
    public ESQueryBuilder shouldIn(String name, Object... items) {
        query().should(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * should range query
     * ? BETWEEN from AND to
     * @param name
     * @param from
     * @param to
     * @return
     */
    public ESQueryBuilder shouldRange(String name, Object from, Object to) {
        query().should(QueryBuilders.rangeQuery(name).from(from).to(to));
        return this;
    }

    // --------------------------------------聚合函数-------------------------------- //
    /**
     * 聚合
     * @param agg
     * @return
     */
    public ESQueryBuilder aggs(AggregationBuilder agg) {
        this.aggs.add(agg);
        return this;
    }

    // --------------------------------------排序-------------------------------- //
    /**
     * ORDER BY sort ASC
     * @param name
     * @return
     */
    public ESQueryBuilder asc(String name) {
        this.sorts.add(SortBuilders.fieldSort(name).order(SortOrder.ASC));
        return this;
    }

    /**
     * ORDER BY sort DESC
     * @param name
     * @return
     */
    public ESQueryBuilder desc(String name) {
        this.sorts.add(SortBuilders.fieldSort(name).order(SortOrder.DESC));
        return this;
    }

    @Override
    public String toString() {
        return toString(-1, -1);
    }

    /**
     * Returns a string of search request dsl
     * 
     * @param from the from, start 0
     * @param size the size, if 0 then miss hits 
     * @return a string of search request dsl
     */
    public String toString(int from, int size) {
        SearchSourceBuilder search = new SearchSourceBuilder();
        if (includeFields != null || excludeFields != null) {
            search.fetchSource(includeFields, excludeFields);
        }
        if (boolQuery != null) {
            search.query(boolQuery);
        }
        for (SortBuilder<?> sort : sorts) {
            search.sort(sort);
        }
        for (AggregationBuilder agg : aggs) {
            search.aggregation(agg); // the aggregation default size 10
        }
        if (from > -1) {
            search.from(from); // default from 0
        }
        if (size > -1) {
            search.size(size); // default size 10
        }

        return search.toString();
    }

    // --------------------------package methods-------------------------
    SearchResponse pagination(Client client, int from, int size) {
        SearchRequestBuilder search = build(client, size);
        search.setSearchType(SearchType.DFS_QUERY_THEN_FETCH); // 深度分布
        search.setFrom(from);
        return search.get();
    }

    SearchResponse scroll(Client client, int size) {
        SearchRequestBuilder search = build(client, size);
        search.setScroll(ElasticSearchClient.SCROLL_TIMEOUT);
        //search.setSearchType(SearchType.QUERY_THEN_FETCH); // default QUERY_THEN_FETCH
        return search.get();
    }

    Aggregations aggregation(Client client) {
        SearchRequestBuilder search = build(client, 0);
        search.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        return search.get().getAggregations();
    }

    // --------------------------private methods-------------------------
    private SearchRequestBuilder build(Client client, int size) {
        //search.request()          -> SearchRequest
        //search.request().source() -> SearchSourceBuilder
        SearchRequestBuilder search = client.prepareSearch(indices);
        if (types != null) {
            search.setTypes(types);
        }
        if (includeFields != null || excludeFields != null) {
            search.setFetchSource(includeFields, excludeFields);
        }
        if (boolQuery != null) {
            search.setQuery(boolQuery);
        }
        for (SortBuilder<?> sort : sorts) {
            search.addSort(sort);
        }
        for (AggregationBuilder agg : aggs) {
            search.addAggregation(agg);
        }
        return search.setSize(size).setExplain(false);
    }

    private BoolQueryBuilder query() {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        return this.boolQuery;
    }

}
