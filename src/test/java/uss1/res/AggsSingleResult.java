package uss1.res;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.google.common.collect.Sets;

import code.ponfee.commons.collect.Collects;

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
        List<Object[]> ds = flat.getAggs().getDataset();
        Object[] dataset = CollectionUtils.isEmpty(ds) ? null : ds.get(0);
        this.aggs = new AggsSingleItem(flat.getAggs().getColumns(), dataset);
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
        if (Sets.newHashSet(fields).size() != fields.length) {
            throw new RuntimeException("Repeat columns: " + Arrays.toString(fields));
        }
        if (   this.getAggs() == null 
            || ArrayUtils.isEmpty(this.getAggs().getColumns()) 
            || ArrayUtils.isEmpty(this.getAggs().getDataset())
        ) {
            return;
        }
        String[] columns = this.getAggs().getColumns(); // 数据列
        Object[] dataset = this.getAggs().getDataset(); // 数据集
        List<String> diff = Collects.different(Arrays.asList(columns), Arrays.asList(fields));
        if (!diff.isEmpty()) {
            throw new RuntimeException("Unknown columns: " + diff + ".");
        }
        List<int[]> swaps = new ArrayList<>();
        for (int i = 0, j, n = fields.length; i < n; i++) { // 以fields为基准
            for (j = i; j < n; j++) {
                if (fields[i].equals(columns[j])) {
                    break;
                }
            }
            if (i != j) {
                Collects.swap(columns, i, j);
                swaps.add(new int[] { i, j });
            }
        }
        if (ArrayUtils.isNotEmpty(dataset) && CollectionUtils.isNotEmpty(swaps)) {
            for (int[] swap : swaps) {
                Collects.swap(dataset, swap[0], swap[1]);
            }
        }
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
