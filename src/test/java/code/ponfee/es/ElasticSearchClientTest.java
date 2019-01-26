package code.ponfee.es;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

public class ElasticSearchClientTest extends BaseTest<ElasticSearchClient> {

    @Test
    public void test1() {
        SearchRequestBuilder search = getBean().prepareSearch("ddt_risk_wastaged", "wastaged_city_es");
        consoleJson(getBean().rankingSearch(search, 10));
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
