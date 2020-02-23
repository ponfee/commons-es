package code.ponfee.es.uss;

import java.util.Map;

import code.ponfee.commons.json.Jsons;
import code.ponfee.es.uss.res.BaseResult;
import code.ponfee.es.uss.res.DataResult;

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

    public <T extends DataResult> T search(SearchPlatform sp, String searchId, String params) {
        return search(sp, searchId, params, null);
    }

    @SuppressWarnings("unchecked")
    public <T extends DataResult> T  search(SearchPlatform sp, String searchId, 
                                            String params, Map<String, String> headers) {
        BaseResult result = SearchRequestBuilder
            .newBuilder(sp, this.url, this.appId, searchId)
            .params(params)
            .headers(headers)
            .build()
            .get();

        if (result.isFailure() || !(result instanceof DataResult)) {
            throw new RuntimeException("USS request failure: " + Jsons.toJson(result));
        }
        return (T) result;
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
