package code.ponfee.es.uss;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.collect.ImmutableMap;

import code.ponfee.commons.model.SearchAfter;
import code.ponfee.commons.model.SortField;
import code.ponfee.es.uss.res.ListResult;


/**
 * SearchAfterUtils instead Page query
 * 
 * @author Ponfee
 */
public class SearchAfterUtils {

    private static final Map<String, String> HEADERS = ImmutableMap.of(ResultConvertor.RESULT_LIST.header(), "X");

    @SuppressWarnings("unchecked")
    public static <E> List<Map<String, Object>> searchAfter(SearchClient client, SearchPlatform type, 
                                                            String searchId, BoolQueryBuilder query, 
                                                            int size, Consumer<SearchSourceBuilder> prepare, 
                                                            SearchAfter<E>... searchAfters) {
        SearchSourceBuilder builder = new SearchSourceBuilder().query(query).size(size);
        Object[] values = new Object[searchAfters.length];
        int i = 0;
        for (SearchAfter<E> sa : searchAfters) {
            SortField sf = sa.getSortField();
            values[i++] = sa.getValue();
            builder.sort(sf.getField(), SortOrder.fromString(sf.getSortOrder().name()));
        }
        builder.searchAfter(values);

        return ((ListResult<Map<String, Object>>) client.search(
            type, searchId, builder.toString(), HEADERS)
        ).getList();
    }

    @SuppressWarnings("unchecked")
    public static <E> void searchFull(SearchClient client, SearchPlatform type, String searchId, 
                                      BoolQueryBuilder query, int size,
                                      Consumer<SearchSourceBuilder> prepare, 
                                      Consumer<List<Map<String, Object>>> consumer, 
                                      SearchAfter<E>... searchStarts) {
        List<Map<String, Object>> each;
        do {
            each = searchAfter(client, type, searchId, query, size, prepare, searchStarts);
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

}
