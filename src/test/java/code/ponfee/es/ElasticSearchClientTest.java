package code.ponfee.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import code.ponfee.commons.collect.Collects;
import code.ponfee.commons.json.Jsons;
import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.model.Result;
import code.ponfee.commons.model.SearchAfter;
import code.ponfee.commons.model.SortField;
import code.ponfee.commons.model.SortOrder;
import code.ponfee.commons.util.ObjectUtils;
import code.ponfee.es.bulk.configuration.BulkProcessorConfiguration;

public class ElasticSearchClientTest extends BaseTest<ElasticSearchClient> {

    @Test
    public void test0() {
        SearchRequestBuilder search = getBean().prepareSearch("ddt_waybill", "ddt_waybill");
        consoleJson(getBean().rankingSearch(search, 10));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test1() {
        int count = 0;
        try {
            Object value1 = "", value2 = 0; // 0L
            String field1 = "waybillNo", field2 = "consignedTime";
            int size = 997;
            List<Map<String, Object>> result;
            do {
                SearchRequestBuilder search = getBean().prepareSearch("ddt_waybill", "ddt_waybill");
                search.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.existsQuery(field2)));
                search.setFetchSource(new String[] { field1,field2 }, null);
                result = getBean().searchAfter(search, size, new SearchAfter<>(new SortField(SortOrder.ASC, field1), value1), new SearchAfter<>(new SortField(SortOrder.ASC, field2), value2));
                if (!result.isEmpty()) {
                    value1 = result.get(result.size() - 1).get(field1);
                    value2 = result.get(result.size() - 1).get(field2);
                    count += result.size();
                }
                //System.out.println(result.stream().map(x -> Objects.toString(x.get(field))).collect(Collectors.joining(",")));
            } while (CollectionUtils.isNotEmpty(result) && result.size() >= size);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(count);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test2() {
        Object value1 = "", value2 = 0; // 0L
        String field1 = "waybillNo", field2 = "consignedTime";
        int size = 997;
        LongAdder la = new LongAdder();
        getBean().searchEnd(
            () ->  getBean().prepareSearch("ddt_waybill", "ddt_waybill").setQuery(QueryBuilders.boolQuery().must(QueryBuilders.existsQuery(field2))).setFetchSource(new String[] { field1, field2 }, null), size, 
            list -> la.add(list.size()), 
            new SearchAfter<>(new SortField(SortOrder.ASC, field1), value1),
            new SearchAfter<>(new SortField(SortOrder.ASC, field2), value2)
        );
        System.out.println(la.longValue());
    }
    
    @Test
    public void test3() {
        //SearchRequestBuilder search = getBean().prepareSearch("ddt_risk_wastaged", "wastaged_city_es");
        SearchRequestBuilder search = getBean().prepareSearch("test_index1", "test_index1");
        consoleJson(getBean().rankingSearch(search, 10));
    }

    @Test
    public void testaddDoc() {
        String json = Jsons.toJson(Collects.toMap("name", "tom", "age", 1, "amount", 100.2));
        String id = getBean().addDoc("test_index1", "test_index1", json);
        console(id);
    }

    @Test
    public void testaddDocWithId() {
        String json = Jsons.toJson(Collects.toMap("name", "tom", "age", 1, "amount", 100.2));
        String id = getBean().addDoc("test_index1", "test_index1", "1", json);
        console(id);
    }

    @Test
    public void testaddDocs() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(Collects.toMap("name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount",  Numbers.scale(new Random().nextDouble()*10000, 2)));
        }
        Result<Void> res = getBean().addDocs("test_index1", "test_index1", list, Jsons::toJson, null);
        console(res);
    }

    @Test
    public void testaddDocsWithId() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(Collects.toMap("name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble()
                * 10000, 2)));
        }
        Result<Void> res = getBean().addDocs("test_index1", "test_index1", list, Jsons::toJson, m -> ObjectUtils.uuid22());
        console(res);
    }

    @Test
    public void testUpdate() {
        getBean().updDoc("test_index1", "test_index1", "1", Jsons.toJson(Collects.toMap("name", "tomxx", "age", 12, "amount", 123.2)));
    }

    @Test
    public void testUpdate2() {
        getBean().updDoc("test_index1", "test_index1", "1", Collects.toMap("name", "tomxx", "age", 112, "amount", 1234.2), Jsons::toJson);
    }

    @Test
    public void testUpdates() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(Collects.toMap("name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
        }
        Result<Void> res = getBean().updDocs("test_index1", "test_index1", list, Jsons::toJson, m->ObjectUtils.uuid22());
        consoleJson(res);
    }
    
    @Test
    public void upsertDocs() {
        List<Map<String, Object>> list = new ArrayList<>();
            list.add(Collects.toMap("id", "AWqb4y6Hgc3LA2ke5vkW", "name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
            list.add(Collects.toMap("id", "aaaaaa", "name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
        Result<Void> res = getBean().upsertDocs("test_index1", "test_index1", list, m -> {Map<String, Object> x= new HashMap<>(m);x.remove("id"); return Jsons.toJson(x);}, m->(String)m.get("id"));
        consoleJson(res);
    }

    @Test
    public void delDoc() {
        console(getBean().delDoc("test_index1", "test_index1", "AWqb4y6Hgc3LA2ke5vkR"));
    }

    @Test
    public void getDocs() {
        consoleJson(getBean().getDocs("test_index1", "test_index1", "AWqb4y6Hgc3LA2ke5vki", "AWqb4y6Hgc3LA2ke5vky"));
    }
    
    @Test
    public void addBulk() {
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(Collects.toMap("id", ObjectUtils.uuid22(), "name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
        list.add(Collects.toMap("id", ObjectUtils.uuid22(), "name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
        
        console(getBean().addBulk("test_index1", "test_index1",list, new BulkProcessorConfiguration(),  m -> {Map<String, Object> x= new HashMap<>(m);x.remove("id"); return Jsons.toJson(x);}, m->(String)m.get("id")));
    }
    
    
    @Test
    public void updBulk() {
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(Collects.toMap("id", "AWqb4y6Hgc3LA2ke5vkW", "name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
        list.add(Collects.toMap("id", "aaaaaa", "name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
        
        console(getBean().updBulk("test_index1", "test_index1",list, new BulkProcessorConfiguration(),  m -> {Map<String, Object> x= new HashMap<>(m);x.remove("id"); return Jsons.toJson(x);}, m->(String)m.get("id")));
    }
    
    @Test
    public void upsertBulk() {
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(Collects.toMap("id", ObjectUtils.uuid22(), "name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
        list.add(Collects.toMap("id", "aaaaaa", "name", RandomStringUtils.randomAlphanumeric(5), "age", new Random().nextInt(99), "amount", Numbers.scale(new Random().nextDouble() * 10000, 2)));
        
        console(getBean().upsertBulk("test_index1", "test_index1",list, new BulkProcessorConfiguration(),  m -> {Map<String, Object> x= new HashMap<>(m);x.remove("id"); return Jsons.toJson(x);}, m->(String)m.get("id")));
    }
    
    public static void main(String[] args) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject() // {
              .startObject("user_mapping") // "user":{ // type name
                .startObject("_ttl") // "_ttl":{ //给记录增加了失效时间，ttl的使用地方如在分布式下（如web系统用户登录状态的维护）
                  .field("enabled", true) // 默认的false的  
                  .field("default", "5m") // 默认的失效时间：d/h/m/s（/小时/分钟/秒）  
                  .field("store", "yes")
                  .field("index", "not_analyzed")
                .endObject() // }
                .startObject("_timestamp") // 表示添加一条索引记录后自动给该记录增加个时间字段（记录的创建时间），供搜索使用
                  .field("enabled", true)
                  .field("store", "no")
                  .field("index", "not_analyzed")
                .endObject() // }
                .startObject("properties") // properties下定义的name为自定义字段，相当于数据库中的表字段 
                  .startObject("@timestamp").field("type", "long").endObject()
                  .startObject("name").field("type", "string").field("store", "yes").endObject()
                  .startObject("home").field("type", "string").field("index", "not_analyzed").endObject()
                  .startObject("now_home").field("type", "string").field("index", "not_analyzed").endObject()
                  .startObject("height").field("type", "double").endObject()
                  .startObject("age").field("type", "integer").endObject()
                  .startObject("birthday").field("type", "date").field("format", "yyyy-MM-dd").endObject()
                  .startObject("isRealMen").field("type", "boolean").endObject()
                  .startObject("location").field("lat", "double").field("lon", "double").endObject()
                .endObject() // }
              .endObject() // }
            .endObject(); // }
        System.out.println(mapping.string());
    }

}
