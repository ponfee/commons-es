package code.ponfee.es.uss;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import code.ponfee.commons.json.Jsons;
import code.ponfee.es.uss.res.ListResult;

public class ListResultTest {

    public static class ListMapDataResult extends ListResult<Map<String, double[]>> {
        private static final long serialVersionUID = 1L;
    }

    public static void main(String[] args) {
        ListMapDataResult res = new ListMapDataResult();
        Map<String, double[]> geo = new HashMap<>();
        geo.put("深圳市", new double[] { 1.0, 2.0 });
        res.addList(Arrays.asList(geo));

        String json = Jsons.toJson(res);
        ListResult<Map<String,double[]>> result = res.fromJson(json);
        System.out.println(result);
    }
}
