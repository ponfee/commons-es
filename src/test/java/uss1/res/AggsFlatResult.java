package uss1.res;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.google.common.collect.Lists;

import uss1.res.AggsTreeResult.AggsTreeItem;

/**
 * USS aggs flat result
 * 
 * @author Ponfee
 */
public class AggsFlatResult extends BaseResult {

    private static final long serialVersionUID = 8416510168590360734L;

    private AggsFlatItem aggs;

    public AggsFlatResult() {}

    public AggsFlatResult(AggsTreeResult aggs) {
        super(aggs);
        if (aggs.getAggs() != null) {
            List<String>       columns = extractColumns(aggs.getAggs());
            List<List<Object>> dataset = extractDataset(aggs.getAggs());
            this.aggs = new AggsFlatItem(
                columns.toArray(new String[columns.size()]), 
                dataset.stream().map(List::toArray).collect(Collectors.toList())
            );
        }
    }

    public AggsFlatItem getAggs() {
        return aggs;
    }

    public void setAggs(AggsFlatItem aggs) {
        this.aggs = aggs;
    }

    private List<String> extractColumns(AggsTreeItem root) {
        List<String> columns = new ArrayList<>();
        LinkedHashMap<String, AggsTreeItem[]> subs = root.getSub();
        while (MapUtils.isNotEmpty(subs)) {
            subs.forEach((k, v) -> columns.add(k));
            AggsTreeItem[] sub = getFirstValue(subs);
            subs = ArrayUtils.isEmpty(sub) ? null : sub[0].getSub();
        }

        return columns;
    }

    private List<List<Object>> extractDataset(AggsTreeItem root) {
        LinkedHashMap<String, AggsTreeItem[]> sub = root.getSub();
        List<List<Object>> dataset = Lists.newArrayList();
        if (MapUtils.isEmpty(getFirstValue(sub)[0].getSub())) {
            dataset.add(
                sub.entrySet().stream().map(
                    e -> e.getValue()[0].getVal()
                ).collect(
                    Collectors.toList()
                )
            );
        } else {
            sub.forEach((k, v) -> {
                Arrays.stream(v).map(this::extractDataset).forEach(dataset::addAll);
            });
        }

        if (root.getKey() != null) {
            dataset.forEach(row -> row.add(0, root.getKey()));
        }
        return dataset;
    }

    private static <K, V> V getFirstValue(Map<K, V> map) {
        return map.entrySet().iterator().next().getValue();
    }

    public static class AggsFlatItem implements Serializable {
        private static final long serialVersionUID = 4286477056501715580L;

        private String[]       columns; // 数据列
        private List<Object[]> dataset; // 数据集

        public AggsFlatItem() {}

        public AggsFlatItem(String[] columns, List<Object[]> dataset) {
            this.columns = columns;
            this.dataset = dataset;
        }

        public String[] getColumns() {
            return columns;
        }

        public void setColumns(String[] columns) {
            this.columns = columns;
        }

        public List<Object[]> getDataset() {
            return dataset;
        }

        public void setDataset(List<Object[]> dataset) {
            this.dataset = dataset;
        }
    }

}
