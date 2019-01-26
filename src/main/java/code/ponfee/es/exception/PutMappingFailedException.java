package code.ponfee.es.exception;

/**
 * PutMappingFailedException
 * 
 * @author Ponfee
 */
public class PutMappingFailedException extends RuntimeException {
    private static final long serialVersionUID = -4073531056239274951L;

    public PutMappingFailedException(String indexName, Throwable cause) {
        super(String.format("Put Mapping failed for Index '%s'", indexName), cause);
    }
}
