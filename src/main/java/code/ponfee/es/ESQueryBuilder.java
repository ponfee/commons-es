package code.ponfee.es;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceRangeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * 查询条件构建器
 * 
 * 明细数据查询与聚合汇总查询：size不指定则默认为10
 * 
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
     * @return caller this
     */
    public ESQueryBuilder mustWildcard(String name, String wildcard) {
        query().must(QueryBuilders.wildcardQuery(name, wildcard));
        return this;
    }

    /**
     * not like
     * 
     * @param name  before or after or both here with *
     * @param wildcard
     * @return caller this
     */
    public ESQueryBuilder mustNotWildcard(String name, String wildcard) {
        query().mustNot(QueryBuilders.wildcardQuery(name, wildcard));
        return this;
    }

    /**
     * Uses script
     * 
     * @param script
     * @return
     */
    public ESQueryBuilder mustScript(String script) {
        query().must(QueryBuilders.scriptQuery(new Script(script)));
        return this;
    }

    /**
     * Uses script
     * 
     * @param script
     * @return
     */
    public ESQueryBuilder mustNotScript(String script) {
        query().mustNot(QueryBuilders.scriptQuery(new Script(script)));
        return this;
    }

    // -------------------------------------------------------------经纬度
    /**
     * 查询[lon,lat]坐标点附件distance米内的数据
     * 
     * @param name the field name
     * @param longitude the longitude
     * @param latitude the latitude
     * @param distance the distance
     * @return this of caller 
     */
    public ESQueryBuilder geoDistance(String name, double longitude, double latitude, double distance) {
        GeoDistanceQueryBuilder geo = QueryBuilders.geoDistanceQuery(name)
                                                   .point(longitude, latitude)
                                                   .distance(distance, DistanceUnit.METERS) // 米
                                                   .geoDistance(GeoDistance.PLANE);
        query().must(geo);
        return this;
    }

    /**
     * 根据两点矩形查询
     * 
     * @param name the field name
     * @param topLat The top latitude
     * @param leftLon The left longitude
     * @param bottomLat The bottom latitude
     * @param rightLon The right longitude
     * @return this of caller
     */
    public ESQueryBuilder geoBoundingBox(String name, double topLat, double leftLon, double bottomLat, double rightLon) {
        query().must(QueryBuilders.geoBoundingBoxQuery(name).setCorners(topLat, leftLon, bottomLat, rightLon));
        return this;
    }

    /**
     * 环形查询
     * 
     * @param name the field name
     * @param lat the latitude
     * @param lon the longitude
     * @param from 内环
     * @param to 外环
     * @return this of caller
     */
    public ESQueryBuilder geoDistanceRange(String name, double lat, double lon, double from, double to) {
        GeoDistanceRangeQueryBuilder queryBuilder = QueryBuilders.geoDistanceRangeQuery(name, lat, lon)
                                                                 .from(from).to(to) // 
                                                                 .includeLower(true).includeUpper(true)
                                                                 .geoDistance(GeoDistance.PLANE);
        query().must(queryBuilder);
        return this;
    }

    /**
     * 多边形查询
     * 
     * @param name the field name
     * @param points the polygon point
     * @return this of caller
     */
    public ESQueryBuilder geoBoundingBox(String name, List<GeoPoint> points) {
        query().must(QueryBuilders.geoPolygonQuery(name, points));
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
        return toString(-1, -1); // default from=0, size=10
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
            search.aggregation(agg); // the aggregation default size=10
        }
        if (from > -1) {
            search.from(from); // default from=0
        }
        if (size > -1) {
            search.size(size); // default size=10
        }

        return search.explain(false).toString();
    }

    // ---------------------------------------------------package methods
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

    // ---------------------------------------------------private methods
    private SearchRequestBuilder build(Client client, int size) {
        //SearchRequestBuilder.request()          -> SearchRequest
        //SearchRequestBuilder.request().source() -> SearchSourceBuilder
        //SearchRequestBuilder.toString()         -> SearchRequestBuilder.request().source().toString()
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
