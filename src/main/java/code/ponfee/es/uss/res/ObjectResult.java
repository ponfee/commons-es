package code.ponfee.es.uss.res;

import java.util.Date;

/**
 * USS object result
 * 
 * @author Ponfee
 */
public class ObjectResult extends BaseResult {

    private static final long serialVersionUID = 5617181318941754226L;

    private Object obj;

    public ObjectResult() {}

    public ObjectResult(boolean status, String errorCode, String errorMessage, Object data) {
        this.setSuccess(status);
        if (!status) {
            this.setErrorCode(errorCode);
            this.setErrorMessage(errorMessage);
        }
        this.setObj(data);
        this.setDate(new Date());
    }

    public ObjectResult(BaseResult other) {
        super(other);
    }

    public ObjectResult(BaseResult other, Object obj) {
        super(other);
        this.obj = obj;
    }

    public ObjectResult(BaseResult other, Object obj, String name) {
        super(other);
        this.obj = obj;
        super.setName(name);
    }

    public static ObjectResult success(Object obj) {
        return success(obj, null);
    }

    public static ObjectResult success(Object obj, String name) {
        ObjectResult result = new ObjectResult();
        result.setSuccess(true);
        result.setDate(new Date());
        result.obj = obj;
        result.setName(name);
        return result;
    }

    public Object getObj() {
        return obj;
    }

    public void setObj(Object obj) {
        this.obj = obj;
    }

}
