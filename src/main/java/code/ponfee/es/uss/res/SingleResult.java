package code.ponfee.es.uss.res;

/**
 * USS single result
 * 
 * @author Ponfee
 */
public class SingleResult<T> extends DataResult {

    private static final long serialVersionUID = 5617181318941754226L;

    private T element;

    public SingleResult() {}

    public SingleResult(DataResult base, T element) {
        super(base);
        this.element = element;
    }

    public T getElement() {
        return element;
    }

    public void setElement(T element) {
        this.element = element;
    }

}
