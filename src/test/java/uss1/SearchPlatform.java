package uss1;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import code.ponfee.commons.http.ContentType;
import code.ponfee.commons.http.Http;
import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.model.Page;
import uss1.req.PageParams;
import uss1.res.AggsFlatResult;
import uss1.res.AggsTreeResult;
import uss1.res.AggsTreeResult.AggsTreeItem;
import uss1.res.BaseResult;
import uss1.res.NormalResult;
import uss1.res.PageMapResult;
import uss1.res.ScrollMapResult;

/**
 * USS（unify search service） Searcher
 * 
 * @author Ponfee
 */
public enum SearchPlatform {

    SEARCH {
        @Override
        protected BaseResult convertResult(NormalResult result, String params, Map<String, String> headers) {
            return convertPageResult(result, parsePageParams(params));
        }
    }, //

    AGGS {
        @Override
        protected BaseResult convertResult(NormalResult result, String params, Map<String, String> headers) {
            return convertAggsResult(result, headers);
        }

        @Override
        public PageParams parsePageParams(String params) {
            throw new UnsupportedOperationException("Aggs search nonuse page params.");
        }
    }, //

    DSL("'{'\"app\":\"{0}\",\"searchId\":{1},\"params\":'{'\"dsl\":{2}'}}'", ImmutableMap.of("version", "1.0")) {
        @Override
        protected BaseResult convertResult(NormalResult result, String params, Map<String, String> headers) {
            Map<String, Object> data = result.getObj();
            if (data.containsKey(AGGS_ROOT)) {
                return convertAggsResult(result, headers);
            } else {
                return convertPageResult(result, parsePageParams(params));
            }
        }
    }, //

    SCROLL(ImmutableMap.of("version", "scroll")) {
        @Override @SuppressWarnings("unchecked")
        protected BaseResult convertResult(NormalResult result, String params, Map<String, String> headers) {
            ScrollMapResult scrollResult = new ScrollMapResult(result);
            Map<String, Object> data = result.getObj();
            scrollResult.setReturnNum(Numbers.toInt(data.get("returnNum")));
            scrollResult.setScrollId(Objects.toString(data.get("scrollId"), ""));
            scrollResult.setList((List<Map<String, Object>>) data.get(HITS_ROOT));
            return scrollResult;
        }

        @Override
        public PageParams parsePageParams(String params) {
            throw new UnsupportedOperationException("Scroll search cannot custom setting page params");
        }
    }, //

    ;

    private static Logger logger = LoggerFactory.getLogger(SearchPlatform.class);

    private static final String AGGS_ROOT = "aggregations";
    private static final String HITS_ROOT = "hits";

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

    // ----------------------------------------------------------request search platform and response
    public String getAsString(String url, String appId, String searchId,
                              String params, Map<String, String> headers) {
        logger.info("USS request params: {}", params);
        String resp = buildHttp(url, appId, searchId, params, headers).request();
        logger.info("USS response body: {}", resp);
        return resp;
    }

    public <T> T get(String url, Class<T> type, String appId, String searchId,
                     String params, Map<String, String> headers) {
        return JSON.parseObject(getAsString(url, appId, searchId, params, headers), type);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getAsMap(String url, String appId, String searchId,
                                        String params, Map<String, String> headers) {
        return get(url, Map.class, appId, searchId, params, headers);
    }

    public BaseResult getAsResult(String url, String appId, String searchId,
                                  String params, Map<String, String> headers) {
        String resp = getAsString(url, appId, searchId, params, headers);
        if (StringUtils.isEmpty(resp)) {
            return BaseResult.failure("Empty response.");
        }
        try {
            NormalResult result = JSON.parseObject(resp, NormalResult.class);
            if (!result.isSuccess() || MapUtils.isEmpty(result.getObj())) {
                return new BaseResult(result);
            }
            result.setTookTime(Numbers.toInt(result.getObj().get("tookTime")));
            result.setHitNum(Numbers.toInt(result.getObj().get("hitNum")));
            return convertResult(result, params, headers);
        } catch (Exception e) {
            logger.warn("BDP-USS search request failure: {}", resp, e);
            return BaseResult.failure(resp);
        }
    }

    protected abstract BaseResult convertResult(NormalResult result, String params, Map<String, String> headers);

    @SuppressWarnings("unchecked")
    public PageParams parsePageParams(String params) {
        Map<String, Object> map = JSON.parseObject(params, Map.class);
        return new PageParams(
            params,
            Numbers.toInt(map.get("from"), PageParams.FROM), 
            Numbers.toInt(map.get("size"), PageParams.SIZE)
        );
    }

    public static SearchPlatform of(String searcher) {
        for (SearchPlatform each : SearchPlatform.values()) {
            if (each.name().equalsIgnoreCase(searcher)) {
                return each;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------private methods
    @SuppressWarnings("unchecked")
    private static PageMapResult convertPageResult(
        NormalResult result, PageParams params) {
        int pageSize = params.getPageSize(), pageNum = params.getPageNum(), from = params.getFrom();
        List<Map<String, Object>> list = (List<Map<String, Object>>) result.getObj().get(HITS_ROOT);
        Page<Map<String, Object>> page = Page.of(list);
        page.setTotal(result.getHitNum());
        page.setPages(PageParams.computeTotalPages(page.getTotal(), pageSize)); // 总页数
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
    private static BaseResult convertAggsResult(NormalResult result, Map<String, String> headers) {
        AggsTreeResult tree = new AggsTreeResult(result);

        // aggregations of search result
        Map<String, Object> aggs = (Map<String, Object>) result.getObj().get(AGGS_ROOT);
        if (MapUtils.isEmpty(aggs)) {
            return tree;
        }

        tree.setAggs(resolveAggregate(aggs));
        //logger.info("tree: {}", JSON.toJSONString(tree, SerializerFeature.DisableCircularReferenceDetect));
        if (headers != null && headers.containsKey(SearchConstants.HEAD_AGGS_TREE)) {
            return tree; // spec search as aggs tree
        } else {
            AggsFlatResult flat = tree.toAggsFlatResult();
            String[] columns = dimColumns(headers);
            if (ArrayUtils.isNotEmpty(columns)) {
                flat.adjustOrders(columns);
            }
            //logger.info("flat: {}", JSON.toJSONString(flat, SerializerFeature.DisableCircularReferenceDetect));
            return flat;
        }
    }

    private static String[] dimColumns(Map<String, String> headers) {
        if (headers == null) {
            return null;
        }

        Object value = headers.get(SearchConstants.HEAD_AGGS_COLUMNS);
        if (value == null) {
            return null;
        }

        if (value instanceof String[]) {
            return (String[]) value;
        } else if (value instanceof Collection) {
            return ((Collection<?>) value).stream().map(x -> x.toString()).toArray(String[]::new);
        } else {
            return value.toString().split(",");
        }
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
     * @param url       the search-platform es http url address, 
     *                  like as http://public-int-gw.intsit.sfdc.com.cn:1080/uss-search/searchService
     * @param appId     the search-platform es http url api appid
     * @param searchId  the search-platform es http url api searchId
     * @param params    the search-platform es request params
     * @param headers   the http request headers
     * @return a Http instance
     */
    private Http buildHttp(String url, String appId, String searchId,
                           String params, Map<String, String> headers) {
        headers = MapUtils.isEmpty(headers) 
                  ? Maps.newHashMap() 
                  : Maps.newHashMap(headers);
        if (this.defaultHeaders != null) {
            this.defaultHeaders.forEach(headers::putIfAbsent);
        }

        return Http.post((url.endsWith("/") ? url : url + "/") + this.urlSuffix)
                   .data(buildRequestBody(appId, searchId, params))
                   .addHeader(headers)
                   .connTimeoutSeconds(60)
                   .readTimeoutSeconds(120)
                   .contentType(ContentType.APPLICATION_JSON);
    }

}
