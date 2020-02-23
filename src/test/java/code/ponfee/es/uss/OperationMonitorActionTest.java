package code.ponfee.es.uss;

import static code.ponfee.es.uss.SearcherConstants.client;
import static code.ponfee.es.uss.SearcherConstants.console;

import java.time.LocalDateTime;
import java.util.Map;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import code.ponfee.es.ESQueryBuilder;
import code.ponfee.es.uss.res.BaseResult;
import code.ponfee.es.uss.res.MapResult;

/**
 * @author Ponfee
 */
public class OperationMonitorActionTest  {

    @Test
    public void totalIncome() {
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustRange("data_date", /*year.concat("01")*/"201801", year.concat(String.format("%02d", date.getMonthValue())))
               .mustEquals("region_flag", "ZB")
               .aggs(AggregationBuilders.sum("total_income").field("income"))
               .aggs(AggregationBuilders.sum("target_income").field("income_target"));
        System.out.println(builder.toString(0,0));
        BaseResult result = client().search(
            SearchPlatform.DSL, "942", builder.toString(0,0), null
        );
        console(result);
    }

    @Test
    public void lastMonthIncome() {
        TermsAggregationBuilder aggs = AggregationBuilders.terms("data_month").field("data_date").size(1).order(Terms.Order.term(false));
        aggs.subAggregation(AggregationBuilders.sum("total_income").field("income"));
        aggs.subAggregation(AggregationBuilders.sum("target_income").field("income_target"));

        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustEquals("region_flag", "ZB")
               .aggs(aggs);
        System.out.println(builder.toString(0,0));
        BaseResult result = client().search(
            SearchPlatform.DSL,  "942", builder.toString(0,0), null
        );
        console(result);
    }
    
    
    @Test
    public void totalProfit() {
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustRange("data_date", /*year.concat("01")*/"201801", year.concat(String.format("%02d", date.getMonthValue())))
               .aggs(AggregationBuilders.sum("total_profit").field("per_tax_prof"))
               .aggs(AggregationBuilders.sum("target_profit").field("prof_target"));
        System.out.println(builder.toString(0,0));
        BaseResult result = client().search(
            SearchPlatform.DSL,  "945", builder.toString(0,0), null
        );
        
        console(result);
    }


    @Test
    public void lastMonthProfit() {
        TermsAggregationBuilder aggs = AggregationBuilders.terms("data_month").field("data_date").size(1).order(Terms.Order.term(false));
        aggs.subAggregation(AggregationBuilders.sum("total_profit").field("per_tax_prof"));
        aggs.subAggregation(AggregationBuilders.sum("target_profit").field("prof_target"));

        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.aggs(aggs);
        System.out.println(builder.toString(0,0));
        BaseResult result = client().search(
            SearchPlatform.DSL,  "945", builder.toString(0,0), null
        );
        console(result);
    }

    @Test
    public void incomeDecreaseRate() {
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustEquals("kind_name", "文化娱乐")
               .mustEquals("industry_name", "文化用品")
               //.must("data_date",  /*year.concat(String.format("%02d", date.getMonthValue()))*/"201812")
               .desc("data_date");
        System.out.println(builder.toString(0,1));
        BaseResult result = client().search(
            SearchPlatform.DSL,  "946", builder.toString(0,1), ImmutableMap.of(ResultConvertor.RESULT_LIST.header(), "1")
        );
        console(result);
    }

    // FIXME ERROR: Cannot support search all: DSL, instead of search after
    // 【中间地图】全快递市场指定月份与一级行业下的各城市件量分布
    @Test @Ignore
    public void areaincomeTargetCompletionRate() {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustEquals("data_date", "201902")
               .mustEquals("region_flag", "DQ")
               .includeFields("region_name", "income_budget_rate");
        System.out.println(builder.toString(0,5000));
        BaseResult result = client().search(
            SearchPlatform.DSL,  "942", builder.toString(0,5000), ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x")
        );
        console(result);
    }

    // ---------------------------------------------------
    // 【中间地图】全快递市场指定月份与一级行业下的各城市件量分布
    @Test
    public void all_kuaidi_industry2_city_data() {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("all_kuaidi_industry2_city_data", "all_kuaidi_industry2_city_data");
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("scity").field("scity").size(Integer.MAX_VALUE).subAggregation(
            AggregationBuilders.sum("total").field("c5")
        );

        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        builder.mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201810")
               .mustEquals("industry1", "医药")
               .aggs(aggs);
        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL,  "983", builder.toString(0,0), null);
        console(result);
    }
    
    // 【右侧饼图】全快递市场每月不同发件规模未开发客户数分类与占比
    @Test
    public void kuaidi_nosf_client_data() {
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("kuaidi_nosf_client_data", "kuaidi_nosf_client_data");
        builder.mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201806")
               .aggs(AggregationBuilders.sum("client1_30").field("client1_30"))
               .aggs(AggregationBuilders.sum("client30_300").field("client30_300"))
               .aggs(AggregationBuilders.sum("client300_1000").field("client300_1000"))
               .aggs(AggregationBuilders.sum("client1000_5000").field("client1000_5000"))
               .aggs(AggregationBuilders.sum("client5000_10k").field("client5000_10k"))
               .aggs(AggregationBuilders.sum("client_10k_up").field("client_10k_up"));

        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL,  "984", builder.toString(0,0), ImmutableMap.of(ResultConvertor.RESULT_ONE.header(), "x"));
        console(result);
    }
    
    // 【左侧柱图】全快递市场的各一级行业件量分布
    @Test
    public void all_kuaidi_industry1_data() {
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("industry1").field("industry1").size(Integer.MAX_VALUE).subAggregation(
            AggregationBuilders.sum("total").field("num_all")
        );
        
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("all_kuaidi_industry1_data", "all_kuaidi_industry1_data");
        builder.mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201802")
               .aggs(aggs);

        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL,  "985", builder.toString(0,0), null);
        console(result);
    }

    // 【左侧柱图】全快递市场的各二级行业件量分布
    @Test
    public void all_kuaidi_industry2_data() {
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("industry2").field("industry2").size(Integer.MAX_VALUE).subAggregation(
            AggregationBuilders.sum("total").field("c3")
        );
        
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("all_kuaidi_industry2_data", "all_kuaidi_industry2_data");
        builder.mustEquals("industry1", "医药")
               .mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201802")
               .aggs(aggs);

        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL,  "986", builder.toString(0,0), null);
        console(result);
    }

    // 【左侧柱图】全快递市场的各一级行业件量分布
    @Test
    public void sf_kuaidi_industry1_data() {
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("industry1").field("industry1").size(Integer.MAX_VALUE).subAggregation(
            AggregationBuilders.sum("total").field("c2")
        );
        
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("sf_kuaidi_industry1_data", "sf_kuaidi_industry1_data");
        builder.mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201802")
               .aggs(aggs);

        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL,  "987", builder.toString(0,0), null);
        console(result);
    }

    // 【左侧柱图】全快递市场的各二级行业件量分布
    @Test
    public void sf_kuaidi_industry2_data() {
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("industry2").field("industry2").size(Integer.MAX_VALUE).subAggregation(
            AggregationBuilders.sum("total").field("c3")
        );
        
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("sf_kuaidi_industry2_data", "sf_kuaidi_industry2_data");
        builder.mustEquals("industry1", "医药")
               .mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201802")
               .aggs(aggs);

        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL,  "988", builder.toString(0,0), null);
        console(result);
    }

    
    // --------------------------------------------------------------
    // 【左侧饼图】顺丰快递的各二级行业非风险客户的收入占比排行前6
    @Test
    public void ddt_rpt_sale_industry_customer_report1() {
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("industry2")
        .field("industry_name")
        //.order(Terms.Order.term(false))
        .size(Integer.MAX_VALUE)
        .subAggregation(
            AggregationBuilders.sum("total").field("all_item_rmb")
        );
        
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_rpt_sale_industry_customer_report", "ddt_rpt_sale_industry_customer_report");
        builder.mustEquals("kind_name", "医药")
               .mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201802")
               .mustNotIn("label_name", "高逾期高理赔客户")
               .aggs(aggs);

        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL,  "988", builder.toString(0,0), null);
        console(result);
    }

    // 【右侧饼图】顺丰快递的各二级行业14种客户类型各自的收入数占比
    @Test
    public void ddt_rpt_sale_industry_customer_report2() {
        TermsAggregationBuilder aggs = 
        AggregationBuilders.terms("label_name")
        .field("label_name")
        .order(Terms.Order.term(false))
        .size(Integer.MAX_VALUE)
        .subAggregation(
            AggregationBuilders.sum("total").field("all_item_rmb")
        );

        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_rpt_sale_industry_customer_report", "ddt_rpt_sale_industry_customer_report");
        builder.mustEquals("kind_name", "医药")
               .mustEquals("industry_name", "医疗器械")
               .mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201902")
               .mustNotIn("label_name", "高逾期高理赔客户")
               .aggs(aggs);

        System.out.println(builder.toString(0,0));

        BaseResult result = client().search(SearchPlatform.DSL,  "1048", builder.toString(0,0), null);
        console(result);
    }

    // FIXME ERROR: Cannot support search all: DSL, instead of search after
    // 【右侧图表】收入段-客户数-客户异动数-客诉率
    @Test @Ignore
    public void ddt_rpt_customer_income_complain() {
        LocalDateTime date = LocalDateTime.now();
        String year = String.valueOf(date.getYear());
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_rpt_customer_income_complain", "ddt_rpt_customer_income_complain");
        builder.mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201902")
               .mustEquals("kind_name", "医药")
               .mustEquals("industry_name", "医疗器械")
               .mustEquals("label_name", "候鸟") // 全部|候鸟
               .asc("income_level");

        System.out.println(builder.toString(0, 5000));

        BaseResult result = client().search(SearchPlatform.DSL,  "1049", builder.toString(0,5000), 
                                            ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x"));
        console(result);
    }

    // 【右侧上方文字】二级行业的所有客户数、收入金额
    @Test
    public void ddt_rpt_sale_industry_customer_income() {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_rpt_sale_industry_customer_income", "ddt_rpt_sale_industry_customer_income");
        builder.mustEquals("kind_name", "医药")
               .mustEquals("industry_name", "医疗器械")
               .mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201902");

        System.out.println(builder.toString(0, 1));

        BaseResult result = client().search(SearchPlatform.DSL,  "1052", builder.toString(0,1), 
                                            ImmutableMap.of(ResultConvertor.RESULT_ONE.header(), "x"));
        console(result);
    }

    // ------------------------------------
    @Test
    public void test() {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_rpt_sale_industry_customer_income", "ddt_rpt_sale_industry_customer_income");
        builder.mustEquals("kind_name", "医药")
               .mustEquals("industry_name", "医疗器械")
               .mustEquals("data_date", /*year.concat(String.format("%02d", date.getMonthValue()))*/"201902");

        console(client().search(SearchPlatform.DSL, "1052", builder.toString(0, 1), null));
        console(client().search(SearchPlatform.DSL, "1052", builder.toString(0, 1), ImmutableMap.of(ResultConvertor.RESULT_ONE.header(), "x")));
        console(client().search(SearchPlatform.DSL, "1052", builder.toString(0, 1), MapResult.class, null));
        console(client().search(SearchPlatform.DSL, "1052", builder.toString(0, 1), Map.class, null));
        console(client().search(SearchPlatform.DSL, "1052", builder.toString(0, 1), String.class, null));
    }
}
