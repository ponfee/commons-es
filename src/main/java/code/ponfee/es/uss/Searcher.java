package code.ponfee.es.uss;

import java.util.Map;

import code.ponfee.es.uss.res.BaseResult;

public class Searcher {

    private final String url;
    private final String appId;

    public Searcher(String url, String appId) {
        this.url = url;
        this.appId = appId;
    }

    public BaseResult search(SearchPlatform type, String searchId, String params) {
        return search(type, searchId, params, null);
    }

    public BaseResult search(SearchPlatform type, String searchId, 
                             String params, Map<String, String> headers) {
        return SearchRequestBuilder
            .newBuilder(type, this.url, this.appId, searchId)
            .params(params)
            .headers(headers)
            .build()
            .getAsResult();
    }

}
