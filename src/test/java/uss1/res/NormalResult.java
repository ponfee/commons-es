package uss1.res;

import java.util.Map;

/**
 * USS normal result
 * 
 * @author Ponfee
 */
public class NormalResult extends BaseResult {

    private static final long serialVersionUID = 5617181318941754226L;

    private Map<String, Object> obj;

    public NormalResult() {}

    public NormalResult(BaseResult other) {
        super(other);
    }

    public NormalResult(BaseResult other, Map<String, Object> obj) {
        super(other);
        this.obj = obj;
    }

    public Map<String, Object> getObj() {
        return obj;
    }

    public void setObj(Map<String, Object> obj) {
        this.obj = obj;
    }

}
