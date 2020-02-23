package code.ponfee.es.uss.res;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import code.ponfee.commons.reflect.BeanMaps;

/**
 * USS scroll map result
 * 
 * @author Ponfee
 */
public class ScrollMapResult extends ScrollResult<Map<String, Object>> {

    private static final long serialVersionUID = 4464963886372526414L;

    public ScrollMapResult() {}

    public ScrollMapResult(BaseResult base, String scrollId) {
        super(base, scrollId);
    }

    // ------------------------------------------------------------
    public <E> ScrollResult<E> transform(Class<E> targetType) {
        return super.transform(map -> BeanMaps.CGLIB.toBean(map, targetType));
    }

    @Override
    public ScrollMapResult fromJson(String json) {
        return JSON.parseObject(
            json, 
            new TypeReference<ScrollMapResult>() {}
        );
    }

}
