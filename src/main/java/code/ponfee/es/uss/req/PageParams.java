package code.ponfee.es.uss.req;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;

/**
 * USS request page params
 * 
 * @author Ponfee
 */
public class PageParams extends BaseParams {

    private static final long serialVersionUID = 1836325388214516038L;

    public static final int FROM = 0;
    public static final int SIZE = 10;
    public static final int PAGE_NUM = 1;
    public static final int PAGE_SIZE = SIZE;

    private int from;
    private int pageNum;
    private int pageSize;
    //private Map<String, org.elasticsearch.search.sort.SortOrder> sortOrder;

    public PageParams(String params, int from, int size) {
        super(params);
        this.from = from;
        this.pageSize = size;
        this.pageNum = computePageNum(from, size);
    }

    public static int computeTotalPages(long totalRecords, int pageSize) {
        return pageSize == 0 ? 0 : (int) ((totalRecords + pageSize - 1) / pageSize);
    }

    public static int computePageNum(long from, int size) {
        return size == 0 ? 0 : (int) from / size + 1;
    }

    public static int computeOffset(long pageNum, int pageSize) {
        return (int) (pageNum - 1) * pageSize;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String buildParams() {
        Map<String, Object> params = new HashMap<>(JSON.parseObject(getParams(), Map.class));
        params.put("from", from);
        params.put("size", pageSize);
        return JSON.toJSONString(params);
    }

}
