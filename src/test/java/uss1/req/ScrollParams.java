package uss1.req;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;

/**
 * USS request scroll params
 * 
 * @author Ponfee
 */
public class ScrollParams extends BaseParams {

    private static final long serialVersionUID = 1241888462624598827L;

    private String scrollId;

    public ScrollParams(String params, String prevScrollId) {
        super(params);
        this.scrollId = prevScrollId;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String buildParams() {
        if (StringUtils.isEmpty(scrollId)) {
            return getParams();
        }
        Map<String, Object> params = JSON.parseObject(getParams(), Map.class);
        params.put("scrollId", scrollId);
        return JSON.toJSONString(params);
    }

}
