package uss1.res;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

/**
 * USS aggs single result
 * 
 * @author 01367825
 */
public class AggsSingleResult extends BaseResult {

    private static final long serialVersionUID = 1495207519018349598L;

    private AggsSingleItem aggs;

    public AggsSingleResult() {}

    public AggsSingleResult(AggsFlatResult aggs) {
        List<Object[]> ds = aggs.getAggs().getDataset();
        Object[] dataset = CollectionUtils.isEmpty(ds) ? null : ds.get(0);
        this.aggs = new AggsSingleItem(aggs.getAggs().getColumns(), dataset);
    }

    public AggsSingleItem getAggs() {
        return aggs;
    }

    public void setAggs(AggsSingleItem aggs) {
        this.aggs = aggs;
    }

    public static class AggsSingleItem implements Serializable {
        private static final long serialVersionUID = -7988376728719260648L;

        private String[] columns; // 数据列
        private Object[] dataset; // 数据值

        public AggsSingleItem() {}

        public AggsSingleItem(String[] columns, Object[] dataset) {
            this.columns = columns;
            this.dataset = dataset;
        }

        public String[] getColumns() {
            return columns;
        }

        public void setColumns(String[] columns) {
            this.columns = columns;
        }

        public Object[] getDataset() {
            return dataset;
        }

        public void setDataset(Object[] dataset) {
            this.dataset = dataset;
        }
    }

}
