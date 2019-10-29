package code.ponfee.es;

import org.elasticsearch.search.SearchHits;

/**
 * 滚动搜索回调函数
 * 
 * @author Ponfee
 */
@FunctionalInterface
public interface ScrollSearchCallback {

    /**
     * 滚动到下一页
     * @param searchHits   搜索结果
     * @param totalRecords 总记录数
     * @param totalPages   总页数
     * @param pageNo       当前滚动的页码
     */
    void nextPage(SearchHits searchHits, long totalRecords, 
                  int totalPages, int pageNo);

    /**
     * 无结果数据
     */
    default void noResult() {}
}
