package code.ponfee.es.uss.res;

import java.beans.Transient;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import code.ponfee.commons.reflect.GenericUtils;

/**
 * USS scroll result
 * 
 * @author Ponfee
 */
public class ScrollResult<T> extends DataResult {

    private static final long serialVersionUID = -7830410131280055362L;

    private final Class<T> type;

    private String scrollId;
    private List<T> list;

    public ScrollResult() {
        this.type = GenericUtils.getActualTypeArgument(this.getClass());
    }

    public ScrollResult(BaseResult base, String scrollId) {
        super(base);
        this.scrollId = scrollId;
        this.type = GenericUtils.getActualTypeArgument(this.getClass());
    }

    /**
     * 判断是否无数据
     * 
     * @return
     */
    @Transient
    public boolean isEmpty() {
        return CollectionUtils.isNotEmpty(list);
    }

    /**
     * 处理
     * 
     * @param action
     */
    public void process(Consumer<T> action) {
        Preconditions.checkArgument(action != null);
        if (CollectionUtils.isNotEmpty(list)) {
            list.stream().forEach(action);
        }
    }

    /**
     * 转换
     * 
     * @param transformer
     * @return
     */
    public <E> ScrollResult<E> transform(Function<T, E> transformer) {
        Preconditions.checkArgument(transformer != null);
        ScrollResult<E> result = new ScrollResult<>(this, this.getScrollId());
        if (CollectionUtils.isNotEmpty(list)) {
            result.setList(
                list.stream().map(transformer).collect(Collectors.toList())
            );
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public <E> ScrollResult<E> copy() {
        return copy((List<E>) list);
    }

    /**
     * 拷贝
     * 
     * @param list
     * @return
     */
    public <E> ScrollResult<E> copy(List<E> list) {
        ScrollResult<E> other = new ScrollResult<>();
        BeanUtils.copyProperties(this, other);
        other.setList(list);
        return other;
    }

    @Override
    public ScrollResult<T> fromJson(String json) {
        return JSON.parseObject(
            json, 
            new TypeReference<ScrollResult<T>>(this.type) {}
        );
    }

    public ListResult<T> toListResult() {
        return new ListResult<>(
            this, 
            Optional.ofNullable(this.getList()).orElse(Lists.newArrayList())
        );
    }

    public int size() {
        return Optional.ofNullable(list).map(List::size).orElse(0);
    }

    // ----------------------------------------------------------getter/setter
    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

}
