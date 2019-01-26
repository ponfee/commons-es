package code.ponfee.es.exception;

/**
 * IndicesExistsFailedException
 * 
 * @author Ponfee
 */
public class IndicesExistsFailedException extends RuntimeException {
    private static final long serialVersionUID = 8042545309351461270L;

    public IndicesExistsFailedException(String indexName, Throwable cause) {
        super(String.format("Indices '%s' failed", indexName), cause);
    }
}
