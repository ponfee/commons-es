package code.ponfee.es.uss.res;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import code.ponfee.es.uss.res.AggsFlatResult.AggsFlatItem;

/**
 * USS aggs single result
 * 
 * 如：饼图时可使用该结构
 * 
 * @author Ponfee
 */
public class AggsSingleResult extends BaseResult {

    private static final long serialVersionUID = 1495207519018349598L;

    private AggsSingleItem aggs;

    public AggsSingleResult() {}

    public AggsSingleResult(AggsFlatResult flat) {
        super(flat);
        List<Object[]> list = flat.getAggs().getDataset();
        Object[] array = CollectionUtils.isEmpty(list) ? null : list.get(0);
        this.aggs = new AggsSingleItem(flat.getAggs().getColumns(), array);
    }

    public AggsSingleItem getAggs() {
        return aggs;
    }

    public void setAggs(AggsSingleItem aggs) {
        this.aggs = aggs;
    }

    /**
     * Adjusts the data orders
     * 
     * @param fields the fields
     */
    public void adjustOrders(String... fields) {
        AggsFlatResult flat = new AggsFlatResult(new AggsFlatItem(
            this.aggs.columns, Collections.singletonList(this.aggs.dataset)
        ));
        flat.adjustOrders(fields);
        this.aggs.columns = flat.getAggs().getColumns();
        this.aggs.dataset = flat.getAggs().getDataset().get(0);
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
