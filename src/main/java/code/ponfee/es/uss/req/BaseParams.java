package code.ponfee.es.uss.req;

import java.io.Serializable;

/**
 * USS request params
 * 
 * @author Ponfee
 */
public class BaseParams implements Serializable {

    private static final long serialVersionUID = -3354822832534935964L;

    private final String params;

    public BaseParams(String params) {
        this.params = params;
    }

    public String getParams() {
        return params;
    }

    public String buildParams() {
        return params;
    }
}
