package code.ponfee.es.bulk.configuration;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;

import code.ponfee.es.bulk.listener.LoggingBulkProcessorListener;
import code.ponfee.es.bulk.options.BulkProcessingOptions;
import code.ponfee.es.bulk.options.BulkProcessingOptionsBuilder;

/**
 * bulk processor配置类
 * 
 * @author Ponfee
 */
public class BulkProcessorConfiguration {

    private final BulkProcessingOptions options;
    private final BulkProcessor.Listener listener;

    public BulkProcessorConfiguration() {
        this(
            BulkProcessingOptionsBuilder.newBuilder("default").build(), 
            new LoggingBulkProcessorListener()
        );
    }

    public BulkProcessorConfiguration(BulkProcessingOptions options) {
        this(options, new LoggingBulkProcessorListener());
    }

    public BulkProcessorConfiguration(BulkProcessingOptions options,
                                      BulkProcessor.Listener listener) {
        this.options = options;
        this.listener = listener;
    }

    public BulkProcessingOptions getBulkProcessingOptions() {
        return options;
    }

    public BulkProcessor.Listener getBulkProcessorListener() {
        return listener;
    }

    // bulk.flush.max.actions：刷新前缓冲的最大动作量
    // bulk.flush.max.size.mb：刷新前缓冲区的最大数据大小（以兆字节为单位）
    // bulk.flush.interval.ms：无论缓冲操作的数量或大小如何都要刷新的时间间隔
    public BulkProcessor build(final Client client) {
        return BulkProcessor.builder(client, listener)
                            .setConcurrentRequests(options.getConcurrentRequests()) // 0
                            .setBulkActions(options.getBulkActions())
                            .setBulkSize(options.getBulkSize())
                            .setFlushInterval(options.getFlushInterval())
                            .setBackoffPolicy(options.getBackoffPolicy())
                            .build();
    }
}
