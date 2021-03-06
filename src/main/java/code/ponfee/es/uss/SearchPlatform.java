package code.ponfee.es.uss;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;

import code.ponfee.commons.http.ContentType;
import code.ponfee.commons.http.Http;
import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.model.Page;
import code.ponfee.commons.model.PageHandler;
import code.ponfee.es.uss.req.PageParams;
import code.ponfee.es.uss.res.AggsFlatResult;
import code.ponfee.es.uss.res.AggsTreeResult;
import code.ponfee.es.uss.res.AggsTreeResult.AggsTreeItem;
import code.ponfee.es.uss.res.BaseResult;
import code.ponfee.es.uss.res.DataResult;
import code.ponfee.es.uss.res.MapResult;
import code.ponfee.es.uss.res.PageMapResult;
import code.ponfee.es.uss.res.ScrollMapResult;

/**
 * USS（unify search service） Searcher
 * 
 * EXCEPTION
 *   EMPTY RESPONSE       ->  BaseResult
 *   PARSE JSON FAIL      ->  BaseResult
 *   RESPONSE ERROR CODE  ->  MapResult
 * 
 * SEARCH
 *  DEFAULT               ->  PageMapResult
 *  X-RESULT-LIST         ->  ListResult
 *  X-RESULT-ONE          ->  SingleResult
 * 
 * SCROLL：
 *  DEFAULT               ->  ScrollMapResult
 * 
 * AGGS
 *   DEFAULT              ->  AggsFlatResult
 *   X-AGGS-TREE          ->  AggsTreeResult
 *   X-RESULT-ONE         ->  AggsSingleResult
 * 
 * DSL
 *   SEARCH, AGGS
 * 
 * @author Ponfee
 */
public enum SearchPlatform {

    SEARCH {
        @Override
        protected PageMapResult convertResult0(MapResult result, String params, Map<String, String> headers) {
            return convertPageResult(result, new PageParams(params));
        }
    }, //

    AGGS {
        @Override
        protected DataResult convertResult0(MapResult result, String params, Map<String, String> headers) {
            return convertAggsResult(result, headers);
        }
    }, //

    DSL("'{'\"app\":\"{0}\",\"searchId\":{1},\"params\":'{'\"dsl\":{2}'}}'", ImmutableMap.of("version", "1.0")) {
        @Override
        protected DataResult convertResult0(MapResult result, String params, Map<String, String> headers) {
            return result.getObj().containsKey(AGGS_ROOT) 
                 ? convertAggsResult(result, headers) 
                 : convertPageResult(result, new PageParams(params));
        }
    }, //

    SCROLL(ImmutableMap.of("version", "scroll")) {
        @Override @SuppressWarnings("unchecked")
        protected ScrollMapResult convertResult0(MapResult result, String params, Map<String, String> headers) {
            ScrollMapResult scrollResult = new ScrollMapResult(result, null);
            Map<String, Object> data = result.getObj();
            String scrollId = (String) data.get("scrollId");
            if (StringUtils.isNotEmpty(scrollId)) {
                scrollResult.setScrollId(scrollId);
            }
            scrollResult.setList((List<Map<String, Object>>) data.get(HITS_ROOT));
            return scrollResult;
        }
    }, //

    ;

    private static Logger logger = LoggerFactory.getLogger(SearchPlatform.class);

    private static final String AGGS_ROOT  = "aggregations";
    private static final String HITS_ROOT  = "hits";
    private static final String HIT_NUM    = "hitNum";

    private final String urlSuffix;
    private final String requestBodyStructure;
    private final ImmutableMap<String, String> defaultHeaders;

    SearchPlatform() {
        this(ImmutableMap.of("version", "1.0"));
    }

    SearchPlatform(ImmutableMap<String, String> defaultHeaders) {
        this("'{'\"app\":\"{0}\",\"searchId\":{1},\"params\":{2}'}'", defaultHeaders);
    }

    SearchPlatform(String requestBodyStructure, ImmutableMap<String, String> defaultHeaders) {
        this.urlSuffix = this.name().toLowerCase();
        this.requestBodyStructure = requestBodyStructure;
        this.defaultHeaders = defaultHeaders;
    }

    public String urlSuffix() {
        return this.urlSuffix;
    }

    public String requestBodyStructure() {
        return this.requestBodyStructure;
    }

    public Map<String, String> defaultHeaders() {
        return this.defaultHeaders;
    }

    public String buildRequestBody(String appId, String searchId, String params) {
        return MessageFormat.format(this.requestBodyStructure, appId, searchId, params);
    }

    // ----------------------------------------------------------request search platform
    @SuppressWarnings("unchecked")
    public <T> T get(String url, Class<T> type, String appId, String searchId,
                     String params, Map<String, String> headers) {
        String resp = request(url, appId, searchId, params, headers);
        return type == String.class ? (T) resp : JSON.parseObject(resp, type);
    }

    @SuppressWarnings("unchecked")
    public <E extends BaseResult> E get(String url, String appId, String searchId,
                                        String params, Map<String, String> headers) {
        String resp = request(url, appId, searchId, params, headers);
        if (StringUtils.isEmpty(resp)) {
            return (E) BaseResult.failure("Empty response.");
        }
        try {
            MapResult result = JSON.parseObject(resp, MapResult.class);
            if (result.isFailure() || MapUtils.isEmpty(result.getObj())) {
                return (E) result;
            }

            return (E) convertResult(result, params, headers);
        } catch (Exception e) {
            logger.warn("BDP-USS search request failure: {}", resp, e);
            return (E) BaseResult.failure(resp);
        }
    }

    public static SearchPlatform of(String name) {
        for (SearchPlatform each : SearchPlatform.values()) {
            if (each.name().equalsIgnoreCase(name)) {
                return each;
            }
        }
        return null;
    }

    // ----------------------------------------------------------protected methods
    protected abstract DataResult convertResult0(MapResult result, String params, Map<String, String> headers);

    // ------------------------------------------------------------------private methods
    private DataResult convertResult(MapResult result, String params, Map<String, String> headers) {
        DataResult dataRes = convertResult0(result, params, headers);
        dataRes.setHitNum(Numbers.toWrapLong(result.getObj().get(HIT_NUM)));
        dataRes.setReturnNum(Numbers.toWrapLong(result.getObj().get("returnNum")));
        dataRes.setTookTime(Numbers.toWrapInt(result.getObj().get("tookTime")));
        return dataRes;
    }

    @SuppressWarnings("unchecked")
    private static PageMapResult convertPageResult(
        MapResult result, PageParams params) {
        int pageSize = params.getPageSize(), pageNum = params.getPageNum(), from = params.getFrom();
        List<Map<String, Object>> list = (List<Map<String, Object>>) result.getObj().get(HITS_ROOT);
        Page<Map<String, Object>> page = Page.of(list);
        page.setTotal(Numbers.toLong(result.getObj().get(HIT_NUM)));
        page.setPages(PageHandler.computeTotalPages(page.getTotal(), pageSize)); // 总页数
        page.setPageNum(pageNum);
        page.setPageSize(pageSize);
        page.setSize(CollectionUtils.isEmpty(list) ? 0 : list.size());
        page.setStartRow(from);
        page.setEndRow(from + page.getSize() - 1);
        page.setFirstPage(pageNum == 1);
        page.setLastPage(pageNum == page.getPages());
        page.setHasNextPage(pageNum > 1);
        page.setHasNextPage(pageNum < page.getPages());
        page.setPrePage(pageNum - 1);
        page.setNextPage(pageNum + 1);

        return new PageMapResult(result, page);
    }

    @SuppressWarnings("unchecked")
    private static DataResult convertAggsResult(MapResult result, Map<String, String> headers) {
        AggsTreeResult tree = new AggsTreeResult(result);

        // agg of dsl result
        Map<String, Object> aggs = (Map<String, Object>) result.getObj().get(AGGS_ROOT);
        if (MapUtils.isEmpty(aggs)) {
            return tree;
        }

        tree.setAggs(resolveAggregate(aggs));
        //logger.debug("tree: {}", JSON.toJSONString(tree, SerializerFeature.DisableCircularReferenceDetect));
        if (headers != null && headers.containsKey(SearchConstants.HEAD_AGGS_TREE)) {
            return tree; // spec search as aggs tree
        } else {
            AggsFlatResult flat = tree.toAggsFlatResult();
            String[] columns = dimColumns(headers);
            if (ArrayUtils.isNotEmpty(columns)) {
                flat.adjustOrders(columns);
            }
            //logger.debug("flat: {}", JSON.toJSONString(flat, SerializerFeature.DisableCircularReferenceDetect));
            return flat;
        }
    }

    private static String[] dimColumns(Map<String, String> headers) {
        if (headers == null) {
            return null;
        }
        String value = headers.get(SearchConstants.HEAD_AGGS_COLUMNS);
        return value == null ? null : value.toString().split(",");
    }

    @SuppressWarnings("unchecked")
    private static AggsTreeItem resolveAggregate(Map<String, Object> rmap) { // root map
        AggsTreeItem root = new AggsTreeItem();
        root.setKey(rmap.get("key"));
        //root.setCnt(Numbers.toInt(rmap.get("doc_count")));

        rmap.forEach((k, v) -> {
            if (v instanceof Map) {
                LinkedHashMap<String, AggsTreeItem[]> sub = root.getSub();
                if (sub == null) {
                    sub = new LinkedHashMap<>();
                    root.setSub(sub);
                }
                Map<String, Object> smap = (Map<String, Object>) v; // sub map
                List<AggsTreeItem> list = new ArrayList<>();
                if (smap.containsKey("buckets")) {
                    ((List<Map<String, Object>>) smap.get("buckets")).forEach(map -> {
                        list.add(resolveAggregate(map));
                    });
                } else {
                    list.add(resolveAggregate(smap));
                }
                sub.put(k, list.toArray(new AggsTreeItem[list.size()]));
            } else if ("value".equals(k) && rmap.size() == 1) {
                root.setVal(v);
            }
        });
        return root;
    }

    /**
     * Requests to uss http api
     * 
     * @param url       the search-platform es http url address
     * @param appId     the search-platform es http url api appid
     * @param searchId  the search-platform es http url api searchId
     * @param params    the search-platform es request params
     * @param headers   the http request headers
     * @return http response string
     */
    private String request(String url, String appId, String searchId,
                           String params, Map<String, String> headers) {
        if (MapUtils.isEmpty(headers)) {
            headers = this.defaultHeaders;
        } else if (MapUtils.isNotEmpty(this.defaultHeaders)) {
            headers = new HashMap<>(headers); // headers maybe immutable or unmodifiable
            headers.putAll(this.defaultHeaders);
        }
        logger.debug("USS request params: {}, headers: {}", params, headers);

        Http http = Http
            .post((url.endsWith("/") ? url : url + "/") + this.urlSuffix)
            .data(buildRequestBody(appId, searchId, params))
            .addHeader(headers)
            .connTimeoutSeconds(60)
            .readTimeoutSeconds(120)
            .contentType(ContentType.APPLICATION_JSON);

        String responseBody = http.request();
        logger.debug(
            "USS response status: {}, headers: {}, body: {}", 
            http.getStatus(), http.getRespHeaders(), responseBody
        );

        return responseBody;
    }

}
