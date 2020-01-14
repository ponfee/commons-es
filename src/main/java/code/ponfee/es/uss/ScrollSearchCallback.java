package code.ponfee.es.uss;

import java.util.List;

/**
 * 滚动搜索回调函数
 * 
 * @author Ponfee
 */
@FunctionalInterface
public interface ScrollSearchCallback<T> {

    /**
     * 滚动到下一页
     * 
     * @param list         搜索结果
     * @param totalRecords 总记录数
     * @param totalPages   总页数
     * @param pageNo       当前滚动的页码
     */
    void nextPage(List<T> list, long totalRecords, int totalPages, int pageNo);

    /**
     * 无结果数据
     */
    default void noResult() {}

}
