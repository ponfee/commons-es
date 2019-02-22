package uss1;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import uss1.res.AggsFlatResult;
import uss1.res.AggsSingleResult;
import uss1.res.BaseResult;
import uss1.res.ListResult;
import uss1.res.ObjectResult;
import uss1.res.PageResult;

/**
 * Constants
 * 
 * @author Ponfee
 */
public enum ResultConvertor {

    RESULT_LIST("X-RESULT-LIST") {
        public BaseResult convert(BaseResult result) {
            return result instanceof PageResult
                ? ((PageResult<?>) result).toListResult()
                : result;
        }
    }, //

    RESULT_ONE("X-RESULT-ONE") {
        public BaseResult convert(BaseResult result) {
            result = RESULT_LIST.convert(result);
            if (result instanceof ListResult) {
                List<?> list = ((ListResult<?>) result).getRows();
                return new ObjectResult(
                    result, CollectionUtils.isEmpty(list) ? null : list.get(0)
                );
            } else if (result instanceof AggsFlatResult) {
                return new AggsSingleResult((AggsFlatResult) result);
            } else {
                return result;
            }
        }
    }, //

    RESULT_NORMAL(null);

    private final String header;

    ResultConvertor(String header) {
        this.header = header;
    }

    public String header() {
        return this.header;
    }

    public BaseResult convert(BaseResult result) {
        return result;
    }

    public static ResultConvertor of(Map<String, String> headers) {
        if (MapUtils.isNotEmpty(headers)) {
            for (ResultConvertor value : ResultConvertor.values()) {
                if (headers.containsKey(value.header)) {
                    return value;
                }
            }
        }
        return RESULT_NORMAL;
    }

}
