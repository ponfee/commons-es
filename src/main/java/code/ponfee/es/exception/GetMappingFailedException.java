package code.ponfee.es.exception;

/**
 * GetMappingFailedException
 * 
 * @author Ponfee
 */
public class GetMappingFailedException extends RuntimeException {
    private static final long serialVersionUID = 713733495290035553L;

    public GetMappingFailedException(String indexName, Throwable cause) {
        super(String.format("Create Mapping failed for Index '%s'", indexName), cause);
    }
}
