package uss1.res;

/**
 * USS object result
 * 
 * @author Ponfee
 */
public class ObjectResult extends BaseResult {

    private static final long serialVersionUID = 5617181318941754226L;

    private Object obj;

    public ObjectResult() {}

    public ObjectResult(BaseResult other) {
        super(other);
    }

    public ObjectResult(BaseResult other, Object obj) {
        super(other);
        this.obj = obj;
    }

    public Object getObj() {
        return obj;
    }

    public void setObj(Object obj) {
        this.obj = obj;
    }

}
