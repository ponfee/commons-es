package code.ponfee.es.bulk.options;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * 批处理操作构建类
 * 
 * @author Ponfee
 */
public class BulkProcessingOptionsBuilder {

    private final String name;

    private int concurrentRequests = 1;
    private int bulkActions = 1000;
    private ByteSizeValue bulkSize = new ByteSizeValue(10, ByteSizeUnit.MB);
    private BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();
    private TimeValue flushInterval = TimeValue.timeValueSeconds(1);

    private BulkProcessingOptionsBuilder(String name) {
        this.name = name;
    }

    public static BulkProcessingOptionsBuilder newBuilder(String name) {
        return new BulkProcessingOptionsBuilder(name);
    }

    public BulkProcessingOptionsBuilder concurrentRequests(int concurrentRequests) {
        this.concurrentRequests = concurrentRequests;
        return this;
    }

    public BulkProcessingOptionsBuilder bulkActions(int bulkActions) {
        this.bulkActions = bulkActions;
        return this;
    }

    public BulkProcessingOptionsBuilder bulkSize(ByteSizeValue bulkSize) {
        this.bulkSize = bulkSize;
        return this;
    }

    public BulkProcessingOptionsBuilder flushInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    public BulkProcessingOptionsBuilder backoffPolicy(BackoffPolicy backoffPolicy) {
        this.backoffPolicy = backoffPolicy;
        return this;
    }

    public BulkProcessingOptions build() {
        return new BulkProcessingOptions(
            name, concurrentRequests, bulkActions, 
            bulkSize, flushInterval, backoffPolicy
        );
    }
}
