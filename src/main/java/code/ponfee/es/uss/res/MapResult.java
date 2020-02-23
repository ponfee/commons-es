package code.ponfee.es.uss.res;

import java.util.Map;

/**
 * USS map result
 * 
 * @author Ponfee
 */
public class MapResult extends BaseResult {

    private static final long serialVersionUID = 5617181318941754226L;

    private Map<String, Object> obj;

    public Map<String, Object> getObj() {
        return obj;
    }

    public void setObj(Map<String, Object> obj) {
        this.obj = obj;
    }

}
