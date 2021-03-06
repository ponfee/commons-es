package code.ponfee.es.uss.res;

import java.beans.Transient;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.beans.BeanUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import code.ponfee.commons.model.Page;
import code.ponfee.commons.reflect.GenericUtils;

/**
 * USS page result
 * 
 * @author Ponfee
 */
public class PageResult<T> extends DataResult {

    private static final long serialVersionUID = 1583479732588220379L;

    private final Class<T> type;
    private Page<T> page;

    public PageResult() {
        this.type = GenericUtils.getActualTypeArgument(this.getClass());
    }

    public PageResult(BaseResult base, Page<T> page) {
        super(base);
        this.page = page;
        this.type = GenericUtils.getActualTypeArgument(this.getClass());
    }

    // ----------------------------------------------------------------public methods
    /**
     * 判断是否无数据
     * 
     * @return
     */
    @Transient
    public boolean isEmpty() {
        return page == null || page.isEmpty();
    }

    /**
     * 处理
     * 
     * @param action
     */
    public void process(Consumer<T> action) {
        Preconditions.checkArgument(action != null);
        if (page != null) {
            page.forEach(action);
        }
    }

    /**
     * 转换
     * 
     * @param mapper
     * @return
     */
    public <E> PageResult<E> transform(Function<T, E> mapper) {
        Preconditions.checkArgument(mapper != null);
        Page<E> pg = page == null ? null : page.map(mapper);
        return new PageResult<>(this, pg);
    }

    @SuppressWarnings("unchecked")
    public <E> PageResult<E> copy() {
        return this.copy((Page<E>) page);
    }

    /**
     * 拷贝
     * 
     * @param page
     * @return
     */
    public <E> PageResult<E> copy(Page<E> page) {
        PageResult<E> other = new PageResult<>();
        BeanUtils.copyProperties(this, other);
        other.setPage(page);
        return other;
    }

    @Override
    public PageResult<T> fromJson(String json) {
        return JSON.parseObject(
            json, 
            new TypeReference<PageResult<T>>(this.type) {}
        );
    }

    public ListResult<T> toListResult() {
        List<T> list = Optional.ofNullable(this.getPage())
                               .map(Page::getRows)
                               .orElse(Lists.newArrayList());
        return new ListResult<>(this, list);
    }

    public int size() {
        return Optional.ofNullable(page).map(Page::getSize).orElse(0);
    }

    // ------------------------------------------------------------getter/setter
    public Page<T> getPage() {
        return page;
    }

    public void setPage(Page<T> page) {
        this.page = page;
    }

}
