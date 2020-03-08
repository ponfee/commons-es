package code.ponfee.es.uss.res;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Preconditions;

import code.ponfee.commons.reflect.GenericUtils;

/**
 * USS list result
 * 
 * @author Ponfee
 */
public class ListResult<T> extends DataResult {

    private static final long serialVersionUID = 1583479732588220379L;

    private final Class<T> type;
    private List<T> list;

    public ListResult() {
        this.type = GenericUtils.getActualTypeArgument(this.getClass());
    }

    public ListResult(BaseResult base, List<T> list) {
        super(base);
        this.list = list;
        this.type = GenericUtils.getActualTypeArgument(this.getClass());
    }

    // ----------------------------------------------------------------public methods
    /**
     * 判断是否无数据
     * 
     * @return
     */
    public @Transient boolean isEmpty() {
        return list == null || list.isEmpty();
    }

    /**
     * 处理
     * 
     * @param action
     */
    public void process(Consumer<T> action) {
        Preconditions.checkArgument(action != null);
        if (list != null) {
            list.forEach(action);
        }
    }

    /**
     * 转换
     * 
     * @param transformer
     * @return
     */
    public <E> ListResult<E> transform(Function<T, E> transformer) {
        Preconditions.checkArgument(transformer != null);
        List<E> ls = (list == null) 
                   ? null 
                   : list.stream().map(transformer).collect(Collectors.toList());
        return new ListResult<>(this, ls);
    }

    @SuppressWarnings("unchecked")
    public <E> ListResult<E> copy() {
        return this.copy((List<E>) list);
    }

    /**
     * 拷贝
     * 
     * @param list
     * @return
     */
    public <E> ListResult<E> copy(List<E> list) {
        ListResult<E> other = new ListResult<>();
        BeanUtils.copyProperties(this, other);
        other.setList(list);
        return other;
    }

    @Override
    public ListResult<T> fromJson(String json) {
        return JSON.parseObject(
            json, 
            new TypeReference<ListResult<T>>(this.type) {}
        );
    }

    public void addList(List<T> data) {
        if (this.list == null) {
            this.list = new ArrayList<>(data.size());
        }
        this.list.addAll(data);
    }

    public int size() {
        return Optional.ofNullable(list).map(List::size).orElse(0);
    }

    // -----------------------------------------------------getter/setter
    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

}
