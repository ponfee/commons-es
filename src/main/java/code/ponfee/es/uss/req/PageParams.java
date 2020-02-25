package code.ponfee.es.uss.req;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;

import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.model.PageHandler;

/**
 * USS page params
 * 
 * @author Ponfee
 */
public class PageParams extends BaseParams {

    private static final long serialVersionUID = 1836325388214516038L;

    private static final int DEFAULT_FROM = 0;  // elasticsearch default from value
    private static final int DEFAULT_SIZE = 10; // elasticsearch default size value

    private final int from;
    private final int pageNum;
    private final int pageSize;
    //private Map<String, org.elasticsearch.search.sort.SortOrder> sortOrder;

    @SuppressWarnings("unchecked")
    public PageParams(String params) {
        super(params);

        Map<String, Object> map = JSON.parseObject(params, Map.class);
        this.from = Numbers.toInt(map.get("from"), PageParams.DEFAULT_FROM);
        this.pageSize = Numbers.toInt(map.get("size"), PageParams.DEFAULT_SIZE);
        this.pageNum = PageHandler.computePageNum(this.from, this.pageSize);
    }

    public int getFrom() {
        return from;
    }

    public int getPageNum() {
        return pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    @Override @SuppressWarnings("unchecked")
    public String buildParams() {
        Map<String, Object> params = StringUtils.isEmpty(getParams()) 
                                   ? new HashMap<>(4) 
                                   : JSON.parseObject(getParams(), Map.class);
        params.put("from", from);
        params.put("size", pageSize);
        return JSON.toJSONString(params);
    }

}
