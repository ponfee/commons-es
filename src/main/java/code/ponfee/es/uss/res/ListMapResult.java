package code.ponfee.es.uss.res;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import code.ponfee.commons.reflect.BeanMaps;

/**
 * USS list map result
 * 
 * @author Ponfee
 */
public class ListMapResult extends ListResult<Map<String, Object>> {

    private static final long serialVersionUID = 1583479732588220379L;

    private static final TypeReference<ListMapResult> TYPE = new TypeReference<ListMapResult>() {};

    // ----------------------------------------------------------------public methods
    /**
     * 转换
     * 
     * @param transformer
     * @return
     */
    public <E> ListResult<E> transform(Class<E> targetType) {
        return super.transform(map -> BeanMaps.CGLIB.toBean(map, targetType));
    }

    @Override
    public ListMapResult fromJson(String json) {
        return JSON.parseObject(json, TYPE);
    }

}
