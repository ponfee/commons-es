package code.ponfee.es.uss;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.util.CollectionUtils;

import code.ponfee.es.uss.res.AggsFlatResult;
import code.ponfee.es.uss.res.AggsSingleResult;
import code.ponfee.es.uss.res.BaseResult;
import code.ponfee.es.uss.res.ListResult;
import code.ponfee.es.uss.res.SingleResult;
import code.ponfee.es.uss.res.PageResult;

/**
 * Result Convertor
 * 
 * @author Ponfee
 */
public enum ResultConvertor {

    RESULT_LIST(SearchConstants.HEAD_RESULT_LIST) {
        @Override
        public BaseResult convert(BaseResult result) {
            return result instanceof PageResult
                ? ((PageResult<?>) result).toListResult()
                : result;
        }
    }, //

    RESULT_ONE(SearchConstants.HEAD_RESULT_ONE) {
        @Override
        public BaseResult convert(BaseResult result) {
            // first convert ListResult if it's a PageResult
            result = RESULT_LIST.convert(result);
            if (result instanceof ListResult) {
                ListResult<?> listRes = (ListResult<?>) result;
                List<?> list = listRes.getList();
                if (CollectionUtils.isEmpty(list)) {
                    return new SingleResult<>(listRes, null);
                } else if (list.size() == 1) {
                    return new SingleResult<>(listRes, list.get(0));
                } else {
                    throw new RuntimeException(
                        "Expected one row (or null), but found: " + list.size()
                    );
                }
            } else if (result instanceof AggsFlatResult) {
                AggsFlatResult flatRes = (AggsFlatResult) result;
                List<Object[]> dataset = flatRes.getAggs().getDataset();
                if (CollectionUtils.isEmpty(dataset) || dataset.size() == 1) {
                    return new AggsSingleResult(flatRes);
                } else {
                    throw new RuntimeException(
                        "Expected one row (or null), but found: " + dataset.size()
                    );
                }
            }

            return result;
        }
    }, //

    RESULT_NOOP(null);

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
        return RESULT_NOOP;
    }

}
