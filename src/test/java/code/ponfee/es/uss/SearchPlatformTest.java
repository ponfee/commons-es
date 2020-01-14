package code.ponfee.es.uss;

import static code.ponfee.es.uss.SearcherConstants.client;
import static code.ponfee.es.uss.SearcherConstants.console;

import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;

import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.model.SearchAfter;
import code.ponfee.commons.model.SortField;
import code.ponfee.commons.model.SortOrder;
import code.ponfee.commons.util.Dates;
import code.ponfee.es.ESQueryBuilder;
import code.ponfee.es.SqlParserClient;
import code.ponfee.es.uss.res.BaseResult;
import code.ponfee.es.uss.res.ListResult;
import code.ponfee.es.uss.res.PageMapResult;
import code.ponfee.es.uss.res.PageResult;

/**
 * @author Ponfee
 */
public class SearchPlatformTest {

    @Test
    public void warehouseArea1() throws SQLFeatureNotSupportedException, SqlParseException {
        long startOfToday = Dates.startOfDay(new Date()).getTime()-864000000;
        long endOfToday = Dates.endOfDay(new Date()).getTime();
        String sql = "SELECT areaName, COUNT(*) cnt FROM ddt_ctes_warehouse_status "
            + "WHERE (recordTime BETWEEN " + startOfToday +" AND " + endOfToday + ") "
            + "AND exceptionType IN ('1','2') GROUP BY areaName LIMIT 999";
        console(client().search(SearchPlatform.DSL, "826", SqlParserClient.get().parseSql(sql), null));
    }

    @Test
    public void warehouseArea2() throws SQLFeatureNotSupportedException, SqlParseException {
        long startOfToday = Dates.startOfDay(new Date()).getTime()-864000000;
        long endOfToday = Dates.endOfDay(new Date()).getTime();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_warehouse_status", "ddt_ctes_warehouse_status");
        TermsAggregationBuilder aggs = AggregationBuilders.terms("areaName").field("areaName").size(Integer.MAX_VALUE)
                           .subAggregation(AggregationBuilders.count("cnt").field("areaName"));
        builder.mustRange("recordTime", startOfToday, endOfToday)
               .mustIn("exceptionType", 1, 2)
               .aggs(aggs);
        console(client().search(SearchPlatform.DSL, "826", builder.toString(0,0), null));
    }

    @Test
    public void warehouseArea3() throws SQLFeatureNotSupportedException, SqlParseException {
        long startOfToday = Dates.startOfDay(new Date()).getTime()-864000000;
        long endOfToday = Dates.endOfDay(new Date()).getTime();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_warehouse_status", "ddt_ctes_warehouse_status");
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("area").field("areaName").size(Integer.MAX_VALUE).subAggregation(
           AggregationBuilders.terms("dept").field("deptName")/*.size(0)*/.subAggregation(
                AggregationBuilders.terms("status").field("exceptionType")/*.size(Integer.MAX_VALUE)*/.subAggregation(
                    AggregationBuilders.count("cnt").field("exceptionType")
                )
            )
        );
        builder.mustRange("recordTime", startOfToday, endOfToday)
               //.mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
               .aggs(aggs);
        //System.out.println(builder.toString(-1,-1));
        BaseResult result = client().search(SearchPlatform.DSL, "826", builder.toString(0,0), null);
        console(result);
        Assert.assertTrue(result.getSuccess());
    }


    @Test
    public void warehouseDetail2() throws SQLFeatureNotSupportedException, SqlParseException {
        long startOfToday = Dates.startOfDay(new Date()).getTime()-864000000;
        long endOfToday = Dates.endOfDay(new Date()).getTime();
        String sql = "SELECT * FROM ddt_ctes_warehouse_status "
            + "WHERE (recordTime BETWEEN " + startOfToday +" AND " + endOfToday + ") "
            + "AND exceptionType IN ('1','2') limit 9";
        //System.out.println( SqlParserClient.get().parseSql(sql));
        console(client().search(SearchPlatform.DSL, "826", SqlParserClient.get().parseSql(sql), null));
    }
    
    
    //----------------------------------------------------------------------------
    @Test
    public void warehouseArea() throws SQLFeatureNotSupportedException, SqlParseException {
        long startOfToday = Dates.startOfDay(new Date()).getTime()-864000000;
        long endOfToday = Dates.endOfDay(new Date()).getTime();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_warehouse_status", "ddt_ctes_warehouse_status");
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("area1").field("areaName").size(Integer.MAX_VALUE).subAggregation(
            AggregationBuilders.terms("status2").field("exceptionType").size(Integer.MAX_VALUE).subAggregation(
                AggregationBuilders.count("cnt3").field("exceptionType")
            )
        );

        builder.mustRange("recordTime", startOfToday, endOfToday)
               //.mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
               .aggs(aggs);
        //System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL, "826", builder.toString(0,0), null);
        console(result);
        Assert.assertTrue(result.getSuccess());
    }

    @Test
    public void vehicleArea() throws SQLFeatureNotSupportedException, SqlParseException {
        long startOfToday = Dates.startOfDay(new Date()).getTime()-864000000;
        long endOfToday = Dates.endOfDay(new Date()).getTime();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_vehicle_status", "ddt_ctes_vehicle_status");
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("area").field("areaName").size(Integer.MAX_VALUE).subAggregation(
            AggregationBuilders.terms("status").field("exceptionType").size(Integer.MAX_VALUE).subAggregation(
                AggregationBuilders.count("cnt").field("exceptionType")
            )
        );

        builder.mustRange("recordTime", startOfToday, endOfToday)
               //.mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
               .aggs(aggs);
        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL, "861", builder.toString(0,0), null);
        console(result);
    }

    @Test
    public void vehicleDetail() throws SQLFeatureNotSupportedException, SqlParseException {
        long startOfToday = Dates.startOfDay(new Date()).getTime() - 864000000;
        long endOfToday = Dates.endOfDay(new Date()).getTime();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_vehicle_status", "ddt_ctes_vehicle_status");

        builder.mustRange("recordTime", startOfToday, endOfToday)
               //.mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
                .desc("recordTime")
                .mustIn("exceptionType", "1", "2");
        //System.out.println(builder.toString(0,10));

        BaseResult result = client().search(SearchPlatform.DSL, "861", builder.toString(0,10), null);
        PageMapResult aggsResult = (PageMapResult) result;
        console(aggsResult);
        Assert.assertTrue(result.getSuccess());
    }

    // ---------------------------------------------------------------------------------
    @Test
    public void warehouseDetail() throws SQLFeatureNotSupportedException, SqlParseException {
        long startOfToday = Dates.startOfDay(new Date()).getTime() - 864000000;
        long endOfToday = Dates.endOfDay(new Date()).getTime();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_warehouse_status", "ddt_ctes_warehouse_status");

        builder.mustRange("recordTime", startOfToday, endOfToday)
               //.mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
                .desc("recordTime")
                .mustIn("exceptionType", "1", "2");
        //System.out.println(builder.toString(0,10));

        BaseResult result = client().search(SearchPlatform.DSL, "826", builder.toString(0,10), null);
        PageMapResult aggsResult = (PageMapResult) result;
        console(aggsResult);
        Assert.assertTrue(result.getSuccess());
    }

    //  FIXME ERROR: not support search all 
    @Test @Ignore
    public void warehouseDetail5() throws SQLFeatureNotSupportedException, SqlParseException {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_warehouse_status", "ddt_ctes_warehouse_status");

        builder
               //.mustRange("recordTime", Dates.startOfDay(new Date()).getTime(), Dates.endOfDay(new Date()).getTime())
               //.mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
               .desc("recordTime")
               //.mustIn("exceptionType", "1", "2")
               ;

        BaseResult result = client().search(SearchPlatform.DSL, "826", builder.toString(0,127), ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x"));
        ListResult<Map<String,Object>> pageResult = (ListResult<Map<String,Object>>) result;
        console(pageResult.getList().size());
        Assert.assertTrue(result.getSuccess());
    }

    // FIXME ERROR: scroll接口不能使用search类型
    @Test @Ignore
    public void warehouseDetail51() throws SQLFeatureNotSupportedException, SqlParseException {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_warehouse_status", "ddt_ctes_warehouse_status");

        builder
               //.mustRange("recordTime", Dates.startOfDay(new Date()).getTime(), Dates.endOfDay(new Date()).getTime())
               //.mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
               .desc("recordTime")
               //.mustIn("exceptionType", "1", "2")
               ;

        BaseResult result = client().search(SearchPlatform.SCROLL, "826", builder.toString(0,127), ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x"));
        if (result instanceof ListResult) {
            console(((ListResult)result).size());
        } else {
            console(result);
        }
        Assert.assertTrue(result.getSuccess());
    }

    @Test
    public void warehouseDetail6() throws SQLFeatureNotSupportedException, SqlParseException {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_warehouse_status", "ddt_ctes_warehouse_status");

        builder
               //.mustRange("recordTime", Dates.startOfDay(new Date()).getTime(), Dates.endOfDay(new Date()).getTime())
               //.mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
               .desc("recordTime")
               //.mustIn("exceptionType", "1", "2")
               ;

        BaseResult result = client().search(SearchPlatform.DSL, "826", builder.toString(0,10000), null);
        PageResult<Map<String,Object>> pageResult = (PageResult<Map<String,Object>>) result;
        console(pageResult.getPage().getSize());
        Assert.assertTrue(result.getSuccess());
    }

    // FIXME ERROR: not support search all 
    @SuppressWarnings("unchecked")
    @Test @Ignore
    public void warehouseDetail7() throws SQLFeatureNotSupportedException, SqlParseException {
        Date now = new Date();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.excludeFields("assetCode", "inputTime")
               .mustRange("recordTime", Dates.startOfDay(now).getTime(), Dates.endOfDay(now).getTime())
               .mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
               .desc("recordTime");

        ListResult<Map<String, Object>> result = (ListResult<Map<String, Object>>) client().search(SearchPlatform.DSL, "826", builder.toString(0, 5000), ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x"));
        List<Map<String, Object>> ls = result.getList().stream().collect(
            Collectors.groupingBy(map -> Objects.toString(map.get("code"), ""))
        ).entrySet().stream().filter(
            e -> StringUtils.isNotEmpty(e.getKey())
        ).map(e -> {
            List<Map<String, Object>> list = e.getValue();
            Map<String, Object> first = list.get(0);
            first.put("except", list.stream().anyMatch(map -> !"3".equals(Objects.toString(map.get("exceptionType"), ""))));
            first.remove("exceptionType");
            first.remove("placeName");
            return first;
        }).collect(Collectors.toList());
        
        console(ls);
        Assert.assertTrue(result.getSuccess());
    }

    // -------------------------------------------------------------------------------------------------
    // SEARCH，指定size：返回size条，from只用于校验，from+size不能超过搜索平台限定的最大值10000，否则报错
    @Test 
    public void vehiclescroll1() throws SQLFeatureNotSupportedException, SqlParseException {
        Date now = new Date();
        Map<String, Object> params = new HashMap<>();
        params.put("min_time", /*Dates.startOfDay(now).getTime()*/0);
        params.put("max_time", Dates.endOfDay(now).getTime());
        params.put("from", 11300); // > 9996，error: size must be less than or equal to: [10000] but was [10001]
        params.put("size", 1);
        Map<String, String> headers = null;
        BaseResult result = client().search(SearchPlatform.SEARCH, "898", JSON.toJSONString(params), headers);
        if (result instanceof ListResult) {
            System.err.println(((ListResult<?>)result).getList().size());
        } else {
            System.err.println(JSON.toJSONString(result));
        }
        Assert.assertTrue(result.getSuccess());
    }

    // FIXME ERROR: not support search all 
    @Test @Ignore
    // SEARCH，error:Batch size is too large, size must be less than or equal to: [10000] but was [10200]. Scroll batch sizes cost as much memory as result windows so they are controlled by the [index.max_result_window] index level setting.
    public void vehiclescroll2() throws SQLFeatureNotSupportedException, SqlParseException {
        Date now = new Date();
        Map<String, Object> params = new HashMap<>();
        params.put("min_time", /*Dates.startOfDay(now).getTime()*/0);
        params.put("max_time", Dates.endOfDay(now).getTime());
        params.put("from", 0);
        params.put("size", 200);
        Map<String, String> headers = new HashMap<>();
        headers.put(SearchConstants.HEAD_SEARCH_ALL, "x");
        headers.put(SearchConstants.HEAD_SCROLL_SIZE, "200");
        BaseResult result = client().search(SearchPlatform.SEARCH, "898", JSON.toJSONString(params), headers);
        if (result instanceof ListResult) {
            System.err.println(((ListResult<?>)result).getList().size());
        } else {
            System.err.println(JSON.toJSONString(result));
        }
    }

    @Test // SCROLL，第一次查询
    public void vehiclescroll3() throws SQLFeatureNotSupportedException, SqlParseException {
        Date now = new Date();
        String params = JSON.toJSONString(ImmutableMap.of("min_time", /*Dates.startOfDay(now).getTime()*/0, "max_time", Dates.endOfDay(now).getTime()));
        Map<String, String> headers = null;
        BaseResult result = client().search(SearchPlatform.SCROLL, "898", params, headers);
        if (result instanceof ListResult) {
            System.err.println(((ListResult<?>)result).getList().size());
        } else {
            System.err.println(JSON.toJSONString(result));
        }
    }

    @Test // SCROLL，查询所有
    public void vehiclescroll4() throws SQLFeatureNotSupportedException, SqlParseException {
        Date now = new Date();
        String params = JSON.toJSONString(ImmutableMap.of("min_time", /*Dates.startOfDay(now).getTime()*/0, "max_time", Dates.endOfDay(now).getTime()));
        Map<String, String> headers = new HashMap<>();
        headers.put(SearchConstants.HEAD_SEARCH_ALL, "x"); // x占位符，没有特殊意义
        headers.put(SearchConstants.HEAD_SCROLL_SIZE, "200");
        BaseResult result = client().search(SearchPlatform.SCROLL, "898", params, headers);
        if (result instanceof ListResult) {
            System.err.println(((ListResult<?>)result).getList().size());
        } else {
            System.err.println(JSON.toJSONString(result));
        }
        Assert.assertTrue(result.getSuccess());
    }

    // ---------------------------------------------------------------------------------
    // FIXME ERROR: not support search all 
    @Test @Ignore
    public void searchStopWarn() {
        Date now = new Date();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.excludeFields("id", "taskId")
               .mustRange("createTm", Dates.plusMinutes(now, -30000).getTime(), Dates.startOfDay(now).getTime())
               .desc("createTm");

        System.out.println(builder.toString(0, 5000));
        BaseResult result = client().search(
            SearchPlatform.DSL, "995", builder.toString(0, 5000), ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x")
        );

        ListResult<Map<String, Object>> res;
        if (result instanceof ListResult) {
            res = (ListResult<Map<String, Object>>) result;
            if (res.getList() == null) {
                res.setList(new ArrayList<>(1));
            }
            res.process(m -> m.put("createTm", Dates.format(new Date(Numbers.toLong(m.get("createTm"))))));
        } else {
            res = new ListResult<>(result, new ArrayList<>(1));
        }
        
        console(res);
        Assert.assertTrue(result.getSuccess());
    }

    @Test(timeout = 600000)
    public void searchafter() { // TODO SUCCESS
        int size = 10000;
        String waybillNo = ""; // start with ""
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("consignedTime").gte(0).lte(Long.MAX_VALUE));
        List<Map<String, Object>> list = null;
        do {
            SearchSourceBuilder searcher = new SearchSourceBuilder().query(query).size(size).sort("waybillNo").searchAfter(new Object[] { waybillNo });
            searcher.fetchSource(new String[] { "consignedTime", "waybillNo", "_uid" }, null);
            BaseResult rs =
                client().search(SearchPlatform.DSL, "1136", searcher.toString(), ImmutableMap.of(ResultConvertor.RESULT_LIST.header(), "x"));
            if (!(rs instanceof ListResult)) {
                System.out.println(JSON.toJSONString(rs));
            }
            list = ((ListResult<Map<String, Object>>) rs).getList();
            waybillNo = CollectionUtils.isEmpty(list) ? null : (String) list.get(list.size() - 1).get("waybillNo");
            System.out.println("=======" + waybillNo + "," + list.size() + "," + rs.getHitNum());
        } while (list.size() == size);
    }

    @Test(timeout = 600000)
    public void searchafter2() { 
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("consignedTime").gte(0).lte(Long.MAX_VALUE));
        List list = SearchAfterUtils.searchAfter(client(), SearchPlatform.DSL, "1136",  query, 100,  search -> search.fetchSource(new String[] { "consignedTime", "waybillNo", "_uid" }, null)
                                     , new SearchAfter<>(new SortField( "waybillNo", SortOrder.ASC), ""));
        System.out.println(JSON.toJSONString(list));
    }
    
    @Test(timeout = 600000)
    public void searchafter3() { 
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("consignedTime").gte(0).lte(Long.MAX_VALUE));
        LongAdder la = new LongAdder();
        Set<String> set = new HashSet<>();
        SearchAfterUtils.searchEnd(client(), SearchPlatform.DSL, "1136",  query, 9999, search -> search.fetchSource(new String[] { "consignedTime", "waybillNo", "_uid" }, null)
                                   , list->{
                                       la.add(list.size());
                                       list.stream().map(m -> (String)m.get("waybillNo")).forEach(set::add);
                                       //System.err.println(list.stream().map(m -> (String)m.get("waybillNo")).collect(Collectors.joining(", ")));
                                       }
                                     , new SearchAfter<>(new SortField("waybillNo", SortOrder.ASC), "00"));
        
        System.out.println(set.size());
        System.out.println(la.longValue());
    }
}
