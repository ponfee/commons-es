package code.ponfee.es.bulk.options;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * 批处理操作类
 * @author fupf
 */
public class BulkProcessingOptions {

    private final String name;
    private final int concurrentRequests;
    private final int bulkActions;
    private final ByteSizeValue bulkSize;
    private final TimeValue flushInterval;
    private final BackoffPolicy backoffPolicy;

    public BulkProcessingOptions(String name, int concurrentRequests, int bulkActions,
                                 ByteSizeValue bulkSize, TimeValue flushInterval,
                                 BackoffPolicy backoffPolicy) {
        this.name = name;
        this.concurrentRequests = concurrentRequests;
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize;
        this.flushInterval = flushInterval;
        this.backoffPolicy = backoffPolicy;
    }

    public String getName() {
        return name;
    }

    public int getConcurrentRequests() {
        return concurrentRequests;
    }

    public int getBulkActions() {
        return bulkActions;
    }

    public ByteSizeValue getBulkSize() {
        return bulkSize;
    }

    public TimeValue getFlushInterval() {
        return flushInterval;
    }

    public BackoffPolicy getBackoffPolicy() {
        return backoffPolicy;
    }

}
