package code.ponfee.es.mapping;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * mapping接口类
 * @author fupf
 */
public interface IElasticSearchMapping {

    XContentBuilder getMapping();

    String getIndexType();

    Version getVersion();

}
