package code.ponfee.es.mapping;

import static java.util.Collections.emptyMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;

import com.google.common.collect.ImmutableMap;

import code.ponfee.es.exception.GetMappingFailedException;

/**
 * Elastic Search Mapping
 * 
 * @author fupf
 */
public abstract class BaseElasticSearchMapping implements IElasticSearchMapping {

    private final String index;
    private final String type;
    private final Version version;

    public BaseElasticSearchMapping(String index, String type, Version version) {
        this.index = index;
        this.type = type;
        this.version = version;
    }

    public @Override XContentBuilder getMapping() {
        try {
            return internalGetMapping();
        } catch(Exception e) {
            throw new GetMappingFailedException(index, e);
        }
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public  Version getVersion() {
        return version;
    }

    public XContentBuilder internalGetMapping() throws IOException {

        // Configure the RootObjectMapper:
        RootObjectMapper.Builder builder = getRootObjectBuilder();

        // Populate the Settings:
        Settings.Builder settingsBuilder = getSettingsBuilder();

        // new Mapping(arg0, arg1, arg2, arg3)
        // Build the Mapping:
        Mapping mapping = new Mapping(
            version,
            builder.build(new Mapper.BuilderContext(settingsBuilder.build(), new ContentPath(1))),
            getMetaDataFieldMappers(),
            getMeta()
        );

        // Turn it into JsonXContent:
        return mapping.toXContent(
            XContentFactory.jsonBuilder().startObject(), new ToXContent.MapParams(emptyMap())
        ).endObject();
    }

    private Settings.Builder getSettingsBuilder() {
        Settings.Builder settingsBuilder = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, version)
                .put(IndexMetaData.SETTING_CREATION_DATE, System.currentTimeMillis());

        configureSettingsBuilder(settingsBuilder);
        return settingsBuilder;
    }

    private RootObjectMapper.Builder getRootObjectBuilder() {
        RootObjectMapper.Builder rootObjectMapperBuilder = new RootObjectMapper.Builder(index + "/" + type);
        configureRootObjectBuilder(rootObjectMapperBuilder);
        return rootObjectMapperBuilder;
    }

    private MetadataFieldMapper[] getMetaDataFieldMappers() {
        List<MetadataFieldMapper> metadataFieldMapper = new ArrayList<>();
        configureMetaDataFieldMappers(metadataFieldMapper);
        return metadataFieldMapper.toArray(new MetadataFieldMapper[metadataFieldMapper.size()]);
    }

    private ImmutableMap<String, Object> getMeta() {
        ImmutableMap.Builder<String, Object> metaFieldsBuilder = new ImmutableMap.Builder<>();
        configureMetaFields(metaFieldsBuilder);
        return metaFieldsBuilder.build();
    }

    protected abstract void configureRootObjectBuilder(RootObjectMapper.Builder builder);

    protected void configureSettingsBuilder(Settings.Builder builder) {}

    protected void configureMetaDataFieldMappers(List<MetadataFieldMapper> metadataFieldMapper) { }

    protected void configureMetaFields(ImmutableMap.Builder<String, Object> metaFieldsBuilder) { }
}
