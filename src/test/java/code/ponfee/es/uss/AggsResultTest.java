package code.ponfee.es.uss;

import static code.ponfee.es.uss.SearcherConstants.client;
import static code.ponfee.es.uss.SearcherConstants.console;

import java.sql.SQLFeatureNotSupportedException;
import java.time.LocalDateTime;
import java.util.Date;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.nlpcn.es4sql.exception.SqlParseException;

import code.ponfee.commons.util.Dates;
import code.ponfee.es.ESQueryBuilder;
import code.ponfee.es.uss.res.BaseResult;

/**
 * @author Ponfee
 */
public class AggsResultTest  {

    @Test
    public void test1() {
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustRange("data_date", /*year.concat("01")*/"201801", year.concat(String.format("%02d", date.getMonthValue())))
               .mustEquals("region_flag", "ZB")
               .aggs(AggregationBuilders.sum("total_income").field("income"))
               .aggs(AggregationBuilders.sum("total_target").field("income_target"));
        //System.out.println(builder.toString(0,0));
        BaseResult result = client().search(
            SearchPlatform.DSL, "942", builder.toString(0,0), null
        );
        console(result);
        Assert.assertTrue(result.getSuccess());
    }

    @Test
    public void test2() {
        LocalDateTime date = LocalDateTime.now();
        TermsAggregationBuilder aggs = AggregationBuilders.terms("month").field("data_date").order(Terms.Order.term(false));
        aggs.subAggregation(AggregationBuilders.sum("total_income").field("income"));
        aggs.subAggregation(AggregationBuilders.sum("total_target").field("income_target"));

        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustRange("data_date", /*year.concat("01")*/"201801", year.concat(String.format("%02d", date.getMonthValue())))
               .mustEquals("region_flag", "ZB")
               .aggs(aggs);
        //System.out.println(builder.toString(0,0));
        BaseResult result = client().search(
            SearchPlatform.DSL, "942", builder.toString(0,0), null
        );
        console(result);
        Assert.assertTrue(result.getSuccess());
    }
    
    @Test
    public void test3() throws SQLFeatureNotSupportedException, SqlParseException {
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
        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL, "826", builder.toString(0,0), null);
        console(result);
        Assert.assertTrue(result.getSuccess());
    }
}
