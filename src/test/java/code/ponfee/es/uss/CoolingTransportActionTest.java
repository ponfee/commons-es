package code.ponfee.es.uss;

import static code.ponfee.es.uss.SearcherConstants.client;
import static code.ponfee.es.uss.SearcherConstants.console;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.util.Dates;
import code.ponfee.es.ESQueryBuilder;
import code.ponfee.es.uss.res.BaseResult;
import code.ponfee.es.uss.res.ListResult;

/**
 * @author Ponfee
 */
public class CoolingTransportActionTest  {

    private static final String APP_ID = "bdp-uss"; 

    // FIXME ERROR: Cannot support search all: DSL, instead of search after
    @Test @Ignore
    public void tmsoverview() {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustRange("statEndTime", 0, Dates.endOfDay(new Date()).getTime());
        System.out.println(builder.toString(0, 5000));
        BaseResult result = client().search(
            SearchPlatform.DSL, "1026", builder.toString(0, 5000), 
            ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x")
        );
        console(result);
        Assert.assertTrue(result.getSuccess());
    }


    // FIXME ERROR: Cannot support search all: DSL, instead of search after
    @Test @Ignore
    public void tmsstatus() {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.mustRange("statEndTime", 0, Dates.endOfDay(new Date()).getTime());
        builder.mustIn("companyName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心");
        System.out.println(builder.toString(0, 5000));
        BaseResult result = client().search(
            SearchPlatform.DSL,"1026", builder.toString(0, 5000), 
            ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x")
        );
        console(result);
        Assert.assertTrue(result.getSuccess());
    }


    private static final List<String> AREA_LIST = Arrays.asList(
        "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心"
    );
    @Test
    public void tmsstatus11() {
        List<String> exceptionTypes = Arrays.asList("1", "2");
        exceptionTypes = null;
        Date now = new Date();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("");
        builder.excludeFields("assetCode", "inputTime")
               .mustRange("recordTime", Dates.startOfDay(now).getTime(), Dates.endOfDay(now).getTime())
               .mustIn("areaName", AREA_LIST)
               .desc("recordTime");
        if (CollectionUtils.isNotEmpty(exceptionTypes)) {
            builder.mustIn("exceptionType", exceptionTypes);
        }

        System.out.println(builder.toString(0, 5000));
        BaseResult result = client().search(
            SearchPlatform.DSL, "1026", builder.toString(0, 5000), null
        );

        if (result instanceof ListResult) {
            ListResult<Map<String, Object>> res = (ListResult<Map<String, Object>>) result;
            if (res.getList() == null) {
                res.setList(new ArrayList<>(1));
            }
            res.process(m -> m.put("recordTime", Dates.format(new Date(Numbers.toLong(m.get("recordTime"))))));
        } else {
        }
       //Assert.assertTrue(result.getSuccess());
    }

    // FIXME ERROR: Cannot support search all: DSL, instead of search after
    @Test @Ignore
    public void tmsflow() {
        Date now = new Date();
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_tms_bill_flow", "ddt_tms_bill_flow");
        builder.excludeFields("statEndTime", "statStartTime")
               .mustExists("carType")
               .mustRange("statEndTime", 0, 1951755177000L);
        System.out.println(builder.toString(0, 5000));
        BaseResult result = client().search(
            SearchPlatform.DSL, "1053", builder.toString(0, 5000), 
            ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x")
        );
        console(result);
        Assert.assertTrue(result.getSuccess());
    }
    
    // FIXME ERROR: Cannot support search all: DSL, instead of search after
    @Test @Ignore
    public void warehouse() {
        ESQueryBuilder builder = ESQueryBuilder.newBuilder("ddt_ctes_warehouse_status", "ddt_ctes_warehouse_status");

        builder
               .mustRange("recordTime", Dates.startOfDay(new Date()).getTime(), Dates.endOfDay(new Date()).getTime())
               .mustIn("areaName", "华北仓配中心", "华东仓配中心", "华南仓配中心", "华西仓配中心", "中南仓配中心")
               .includeFields("cityName")
               ;
        System.out.println(builder.toString(0,5000));
        BaseResult result = client().search(SearchPlatform.DSL, "826", builder.toString(0,5000), ImmutableMap.of(SearchConstants.HEAD_SEARCH_ALL, "x"));
        if (result instanceof ListResult) {
            console(result);
        } else {
            console(result);
        }
        Assert.assertTrue(result.getSuccess());
    }
}
