package code.ponfee.es.mapping;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * mapping接口类
 * 
 * @author Ponfee
 */
public interface ElasticSearchMapping {

    String getIndex();

    String getType();

    XContentBuilder getMapping();

    Version getVersion();

}
