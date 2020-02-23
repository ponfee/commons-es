package code.ponfee.es.uss;

import java.util.Map;

import code.ponfee.commons.json.Jsons;
import code.ponfee.es.uss.res.BaseResult;

/**
 * Searcher
 * 
 * @author Ponfee
 */
public class Searcher {

    private final String url;
    private final String appId;

    public Searcher(String url, String appId) {
        this.url = url;
        this.appId = appId;
    }

    public <T extends BaseResult> T search(SearchPlatform sp, String searchId, String params) {
        return search(sp, searchId, params, null);
    }

    public <T extends BaseResult> T  search(SearchPlatform sp, String searchId, 
                                            String params, Map<String, String> headers) {
        T result = SearchRequestBuilder
            .newBuilder(sp, this.url, this.appId, searchId)
            .params(params)
            .headers(headers)
            .build()
            .get();

        if (result.isFailure()) {
            throw new RuntimeException("USS request failure: " + Jsons.toJson(result));
        }
        return result;
    }

    public <T> T search(SearchPlatform sp, String searchId,
                        String params, Class<T> type, 
                        Map<String, String> headers) {
        return SearchRequestBuilder
            .newBuilder(sp, this.url, this.appId, searchId)
            .params(params)
            .headers(headers)
            .build()
            .get(type);
    }

}
