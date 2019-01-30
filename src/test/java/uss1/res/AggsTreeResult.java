package uss1.res;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * USS aggs tree result
 * 
 * @author Ponfee
 */
public class AggsTreeResult extends BaseResult {

    private static final long serialVersionUID = -8508458241937662387L;

    private AggsTreeItem aggs; // root

    public AggsTreeResult() {}

    public AggsTreeResult(BaseResult base) {
        super(base);
    }

    public AggsTreeItem getAggs() {
        return aggs;
    }

    public void setAggs(AggsTreeItem aggs) {
        this.aggs = aggs;
    }

    public AggsFlatResult toAggsFlatResult() {
        return new AggsFlatResult(this);
    }

    public static class AggsTreeItem implements Serializable {
        private static final long serialVersionUID = 8638064268630624314L;

        private LinkedHashMap<String, AggsTreeItem[]> sub; // sub aggs
        private Object                                key; // key
        private Object                                val; // value

        public LinkedHashMap<String, AggsTreeItem[]> getSub() {
            return sub;
        }

        public void setSub(LinkedHashMap<String, AggsTreeItem[]> sub) {
            this.sub = sub;
        }

        public Object getKey() {
            return key;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public Object getVal() {
            return val;
        }

        public void setVal(Object val) {
            this.val = val;
        }
    }

}
