package uss1.res;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import code.ponfee.commons.util.ObjectUtils;

/**
 * USS list map result
 * 
 * @author Ponfee
 */
public class ListMapResult extends ListResult<Map<String, Object>> {

    private static final long serialVersionUID = 1583479732588220379L;

    public ListMapResult() {
        this(null, null);
    }

    public ListMapResult(BaseResult base) {
        this(base, null);
    }

    public ListMapResult(BaseResult base, List<Map<String, Object>> list) {
        super(base, list);
    }

    // ----------------------------------------------------------------public methods
    /**
     * 转换
     * 
     * @param transformer
     * @return
     */
    public <E> ListResult<E> transform(Class<E> targetType) {
        return super.transform(map -> ObjectUtils.map2bean(map, targetType));
    }

    @Override
    public ListMapResult fromJson(String json) {
        return JSON.parseObject(
            json, 
            new TypeReference<ListMapResult>() {}
        );
    }

}
