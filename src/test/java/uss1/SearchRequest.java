package uss1;

import static uss1.SearchConstants.HEAD_SCROLL_SIZE;
import static uss1.SearchConstants.HEAD_SEARCH_ALL;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.util.Holder;
import uss1.req.PageParams;
import uss1.req.ScrollParams;
import uss1.res.BaseResult;
import uss1.res.ListResult;
import uss1.res.PageResult;
import uss1.res.ScrollResult;

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

    public String getAsString() {
        return searcher.getAsString(url, appId, searchId, params, headers);
    }

    public <T> T getAsBean(Class<T> beanType) {
        return searcher.get(url, beanType, appId, searchId, params, headers);
    }

    public Map<String, Object> getAsMap() {
        return searcher.getAsMap(url, appId, searchId, params, headers);
    }

    @SuppressWarnings("unchecked")
    public <T> BaseResult getAsResult() {
        if (hasHeader(HEAD_SEARCH_ALL)) {
            Holder<BaseResult> retn = Holder.empty();
            List<T> data = new LinkedList<>();
            scrollSearch(
                retn::getAndSet, 
                (list, total, pages, no) -> data.addAll((List<T>) list)
            );

            BaseResult result = retn.get();
            if (   result instanceof ScrollResult
                || result instanceof PageResult
            ) {
                return new ListResult<>(result, data);
            } else {
                return result;
            }
        }

        return ResultConvertor.of(headers).convert(
            searcher.getAsResult(url, appId, searchId, params, headers)
        );
    }

    public <T> void scrollSearch(ScrollSearchConsumer<T> callback) {
        scrollSearch(null, callback); // accept null: prevent non scroll search
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
    private <T> void scrollSearch(Consumer<BaseResult> accept, ScrollSearchConsumer<T> callback) {
        BaseResult result = searcher.getAsResult(url, appId, searchId, params, headers);
        if (accept != null) {
            accept.accept(result);
        }

        if (result instanceof ScrollResult) {
            ScrollResult<T> scrollResult = (ScrollResult<T>) result;
            if (scrollResult.size() < 1) {
                callback.noResult();
                return;
            }

            int totalRecords = scrollResult.getHitNum(),
                // scrollSize由创建接口时指定（单次返回条数），only use in compute total pages
                scrollSize = Optional.ofNullable(getHeader(HEAD_SCROLL_SIZE)).map(Numbers::toInt).orElse(scrollResult.getList().size()),
                totalPages = PageParams.computeTotalPages(totalRecords, scrollSize), 
                pageNo = 1;
            callback.nextPage(scrollResult.getList(), totalRecords, totalPages, pageNo++);

            while (scrollResult.size() >= scrollSize) {
                scrollResult = (ScrollResult<T>) searcher.getAsResult(
                    url, appId, searchId, new ScrollParams(params, scrollResult.getScrollId()).buildParams(), headers
                );
                if (scrollResult.size() > 0) {
                    callback.nextPage(scrollResult.getList(), totalRecords, totalPages, pageNo++);
                }
            }

        } else if (result instanceof PageResult) {
            PageResult<T> pageResult = (PageResult<T>) result;
            if (pageResult.size() < 1) {
                callback.noResult();
                return;
            }

            PageParams pageParams = searcher.parsePageParams(params);
            int totalRecords = pageResult.getHitNum(), 
                pageSize = pageParams.getPageSize(),
                totalPages = PageParams.computeTotalPages(totalRecords, pageSize), 
                pageNo = 1, from = pageParams.getFrom();
            callback.nextPage(pageResult.getPage().getRows(), totalRecords, totalPages, pageNo++);

            while (pageResult.size() >= pageSize) {
                pageParams.setFrom(from += pageResult.size());
                pageResult = (PageResult<T>) searcher.getAsResult(
                    url, appId, searchId, pageParams.buildParams(), headers
                );
                if (pageResult.size() > 0) {
                    callback.nextPage(pageResult.getPage().getRows(), totalRecords, totalPages, pageNo++);
                }
            }

        } else if (accept == null) {
            throw new UnsupportedOperationException("Cannot support search all: " + searcher.name());
        }
    }

}
