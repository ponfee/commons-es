package code.ponfee.es.uss;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.model.PageHandler;
import code.ponfee.es.uss.req.ScrollParams;
import code.ponfee.es.uss.res.BaseResult;
import code.ponfee.es.uss.res.ListResult;
import code.ponfee.es.uss.res.ScrollResult;

/**
 * USS search request
 * 
 * @author Ponfee
 */
public class SearchRequest {

    // ---------------------------------require
    private final SearchPlatform searcher;
    private final String url;
    private final String appId;
    private final String searchId;

    // ---------------------------------optinal
    private final String params;
    private final Map<String, String> headers;

    SearchRequest(SearchPlatform searcher, 
                  String url, String appId, 
                  String searchId, String params, 
                  Map<String, String> headers) {
        this.searcher = searcher;
        this.url = url;
        this.appId = appId;
        this.searchId = searchId;
        this.params = params;
        this.headers = headers;
    }

    // ---------------------------------search api
    public <T> T get(Class<T> type) {
        return searcher.get(url, type, appId, searchId, params, headers);
    }

    @SuppressWarnings("unchecked")
    public <T, E extends BaseResult> E get() {
        if (hasHeader(SearchConstants.HEAD_SEARCH_ALL)) {
            FullSearchAccepter<T> fullSearch = new FullSearchAccepter<>();
            BaseResult result = scrollSearch(fullSearch);
            return fullSearch.toResult(result);
        } else {
            return (E) ResultConvertor.of(headers).convert(
                searcher.get(url, appId, searchId, params, headers)
            );
        }
    }

    public <T> void scroll(ScrollSearchAccepter<T> callback) {
        scrollSearch(callback);
    }

    // -------------------------------------------------------------------private methods
    private boolean hasHeader(String header) {
        return headers != null && headers.containsKey(header);
    }

    private String getHeader(String header) {
        return getHeader(header, null);
    }

    private String getHeader(String header, String defaultVal) {
        return (headers == null || header == null) 
               ? defaultVal : headers.get(header);
    }

    @SuppressWarnings("unchecked")
    private <T> BaseResult scrollSearch(ScrollSearchAccepter<T> callback) {
        BaseResult result = searcher.get(url, appId, searchId, params, headers);

        if (result instanceof ScrollResult) {
            ScrollResult<T> scrollRes = (ScrollResult<T>) result;
            if (scrollRes.size() < 1) {
                callback.noResult();
                return result;
            }

            long totalRecords = scrollRes.getHitNum();

            // scrollSize由创建接口时指定（单次返回条数），only use in compute total pages
            int scrollSize = Optional.ofNullable(getHeader(SearchConstants.HEAD_SCROLL_SIZE))
                                     .map(Numbers::toInt).orElse(scrollRes.getList().size()),
                totalPages = PageHandler.computeTotalPages(totalRecords, scrollSize), 
                pageNo = 1;
            callback.nextPage(scrollRes.getList(), totalRecords, totalPages, pageNo++);

            while (scrollRes.size() >= scrollSize) {
                scrollRes = (ScrollResult<T>) searcher.get(
                    url, appId, searchId, 
                    new ScrollParams(params, scrollRes.getScrollId()).buildParams(), 
                    headers
                );
                if (scrollRes.size() > 0) {
                    callback.nextPage(scrollRes.getList(), totalRecords, totalPages, pageNo++);
                }
            }

        } else {
            // 超过index.max_result_window会报错，请勿使用（务必使用SCROLL或search-after）
            throw new UnsupportedOperationException("Cannot support full search: " + searcher.name());

            /*
            PageResult<T> pageResult = (PageResult<T>) result;
            if (pageResult.size() < 1) {
                callback.noResult();
                return result;
            }

            PageParams pageParams = searcher.parsePageParams(params);
            long totalRecords = pageResult.getHitNum(); 
            int pageSize = pageParams.getPageSize(),
                totalPages = PageParams.computeTotalPages(totalRecords, pageSize), 
                pageNo = 1, from = pageParams.getFrom();
            callback.nextPage(pageResult.getPage().getRows(), totalRecords, totalPages, pageNo++);

            while (pageResult.size() >= pageSize) {
                pageParams.setFrom(from += pageResult.size());
                pageResult = (PageResult<T>) searcher.get(
                    url, appId, searchId, pageParams.buildParams(), headers
                );
                if (pageResult.size() > 0) {
                    callback.nextPage(pageResult.getPage().getRows(), totalRecords, totalPages, pageNo++);
                }
            }
            */
        }
        return result;
    }

    private static class FullSearchAccepter<T> implements ScrollSearchAccepter<T> {
        private boolean noResult = false;
        private List<T> data = new LinkedList<>();

        @Override
        public void nextPage(List<T> list, long totalRecords, int totalPages, int pageNo) {
            data.addAll(list);
        }

        @Override
        public void noResult() {
            noResult = true;
        }

        @SuppressWarnings("unchecked")
        public <E extends BaseResult> E toResult(BaseResult base) {
            return (E) (noResult ? base : new ListResult<>(base, data));
        }
    }

}
