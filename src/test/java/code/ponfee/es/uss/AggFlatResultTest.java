package code.ponfee.es.uss;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;

import code.ponfee.es.uss.res.AggsFlatResult;
import code.ponfee.es.uss.res.AggsFlatResult.AggsFlatItem;

public class AggFlatResultTest {

    @Test
    public void test() {
        String[] dimensions = { "a", "b", "c", "d" };
        AggsFlatResult result = new AggsFlatResult();

        String[] columns = Arrays.copyOf(dimensions, dimensions.length);
        List<String> list = Lists.newArrayList(columns);
        Collections.shuffle(list);
        columns = list.toArray(new String[0]);
        List<Object[]> data = Lists.newArrayList();
        data.add(new Object[] { columns[0], columns[1], columns[2], columns[3] });
        data.add(new Object[] { columns[0], columns[1], columns[2], columns[3] });
        data.add(new Object[] { columns[0], columns[1], columns[2], columns[3] });
        AggsFlatItem aggs = new AggsFlatItem(columns, data);
        result.setAggs(aggs);

        System.out.println(JSON.toJSONString(aggs.getColumns()));
        System.out.println(JSON.toJSONString(aggs.getDataset()));
        System.out.println("============");
        result.adjustOrders(dimensions);
        System.out.println(JSON.toJSONString(aggs.getColumns()));
        System.out.println(JSON.toJSONString(aggs.getDataset()));
    }

}
