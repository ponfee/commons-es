package uss1;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.collect.ImmutableMap;

import code.ponfee.commons.model.SearchAfter;
import code.ponfee.commons.model.SortField;
import uss1.res.BaseResult;
import uss1.res.ListResult;

/**
 * SearchAfterUtils instead Page query
 * 
 * @author Ponfee
 */
public class SearchAfterUtils {

    private static final Map<String, String> HEADERS = ImmutableMap.of(ResultConvertor.RESULT_LIST.header(), "x");

    @SuppressWarnings("unchecked")
    public static <E> List<Map<String, Object>> searchAfter(BoolQueryBuilder query, int size, 
                                                            Consumer<SearchSourceBuilder> prepare, 
                                                            SearchAfter<E>... searchAfters) {
        SearchSourceBuilder search = new SearchSourceBuilder().query(query).size(size);
        Object[] values = new Object[searchAfters.length];
        int i = 0;
        for (SearchAfter<E> sa : searchAfters) {
            SortField sf = sa.getSortField();
            values[i++] = sa.getValue();
            search.sort(sf.getField(), SortOrder.fromString(sf.getSortOrder().name()));
        }
        search.searchAfter(values);

        return ((ListResult<Map<String, Object>>) search(
            SearchPlatform.DSL, search.toString(), HEADERS)
        ).getList();
    }

    @SuppressWarnings("unchecked")
    public static <E> void searchAll(BoolQueryBuilder query, int size,
                                     Consumer<SearchSourceBuilder> prepare, 
                                     Consumer<List<Map<String, Object>>> consumer, 
                                     SearchAfter<E>... searchStarts) {
        List<Map<String, Object>> each;
        do {
            each = searchAfter(query, size, prepare, searchStarts);
            if (!each.isEmpty()) {
                Map<String, Object> endRow = each.get(each.size() - 1);
                for (int i = 0; i < searchStarts.length; i++) {
                    SearchAfter<E> sa = searchStarts[i];
                    searchStarts[i] = new SearchAfter<E>(
                        sa.getSortField(), (E) endRow.get(sa.getSortField().getField())
                    );
                }
                consumer.accept(each);
            }
        } while (each.size() == size);
    }

    private static BaseResult search(SearchPlatform searcher, String params, Map<String, String> headers) {
        return SearchRequestBuilder
                .newBuilder(searcher, "URL", "appId", "searchId")
                .params(params).headers(headers).build()
                .getAsResult();
    }
}
