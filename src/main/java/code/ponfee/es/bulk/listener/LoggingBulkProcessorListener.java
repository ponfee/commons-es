package code.ponfee.es.bulk.listener;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 批处理监听
 * 
 * @author Ponfee
 */
public class LoggingBulkProcessorListener implements BulkProcessor.Listener {

    private static final Logger logger = LoggerFactory.getLogger(LoggingBulkProcessorListener.class);

    public LoggingBulkProcessorListener() {}

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        logger.debug(
            "ExecutionId = {}, Actions = {}, Estimated Size = {}",
            executionId, request.numberOfActions(), request.estimatedSizeInBytes()
        );
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        logger.debug(
            "ExecutionId = {}, Actions = {}, Estimated Size = {}",
            executionId, request.numberOfActions(), request.estimatedSizeInBytes()
        );
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        logger.error("ExecutionId = {}, Error = {}", executionId, failure);
    }
}
