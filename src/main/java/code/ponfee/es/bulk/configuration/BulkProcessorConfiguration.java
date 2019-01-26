package code.ponfee.es.bulk.configuration;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;

import code.ponfee.es.bulk.listener.LoggingBulkProcessorListener;
import code.ponfee.es.bulk.options.BulkProcessingOptions;
import code.ponfee.es.bulk.options.BulkProcessingOptionsBuilder;

/**
 * bulk processor配置类
 * @author fupf
 */
public class BulkProcessorConfiguration {

    private BulkProcessingOptions options = new BulkProcessingOptionsBuilder().build();
    private BulkProcessor.Listener listener = new LoggingBulkProcessorListener();

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

    public BulkProcessor build(final Client client) {
        return BulkProcessor.builder(client, listener)
                            .setConcurrentRequests(options.getConcurrentRequests())
                            .setBulkActions(options.getBulkActions())
                            .setBulkSize(options.getBulkSize())
                            .setFlushInterval(options.getFlushInterval())
                            .setBackoffPolicy(options.getBackoffPolicy())
                            .build();
    }
}
