package code.ponfee.es.uss.res;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import code.ponfee.commons.model.Page;
import code.ponfee.commons.reflect.BeanMaps;

/**
 * USS page map result
 * 
 * @author Ponfee
 */
public class PageMapResult extends PageResult<Map<String, Object>> {

    private static final long serialVersionUID = -2042611387612676297L;

    private static final TypeReference<PageMapResult> TYPE = new TypeReference<PageMapResult>() {};

    public PageMapResult() {}

    public PageMapResult(BaseResult base, Page<Map<String, Object>> page) {
        super(base, page);
    }

    // ------------------------------------------------------------
    public <E> PageResult<E> transform(Class<E> targetType) {
        return super.transform(map -> BeanMaps.CGLIB.toBean(map, targetType));
    }

    @Override
    public PageMapResult fromJson(String json) {
        return JSON.parseObject(json, TYPE);
    }

}
