package code.ponfee.es.uss;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;

/**
 * USS search request builder
 * 
 * @author Ponfee
 */
public class SearchRequestBuilder {

    private final SearchPlatform searcher;
    private final String url;
    private final String appId;
    private final String searchId;

    private String params = "{}";
    private Map<String, String> headers;

    private SearchRequestBuilder(SearchPlatform searcher, String url, 
                                 String appId, String searchId) {
        this.searcher = searcher;
        this.url = url;
        this.appId = appId;
        this.searchId = searchId;
    }

    public static SearchRequestBuilder newBuilder(SearchPlatform searcher, String url, 
                                                  String appId, String searchId) {
        return new SearchRequestBuilder(searcher, url, appId, searchId);
    }

    public SearchRequestBuilder params(String params) {
        if (StringUtils.isNotBlank(params)) {
            this.params = params;
        }
        return this;
    }

    public SearchRequestBuilder params(Map<String, String> params) {
        if (MapUtils.isNotEmpty(params)) {
            this.params = JSON.toJSONString(params);
        }
        return this;
    }

    public SearchRequestBuilder headers(Map<String, String> headers) {
        if (MapUtils.isNotEmpty(headers)) {
            this.headers = headers;
        }
        return this;
    }

    public SearchRequest build() {
        return new SearchRequest(searcher, url, appId, searchId, params, headers);
    }

}
