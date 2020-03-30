package code.ponfee.es;

import static org.apache.commons.lang3.StringUtils.split;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import code.ponfee.commons.json.Jsons;
import code.ponfee.commons.model.Page;
import code.ponfee.commons.model.PageHandler;
import code.ponfee.commons.model.Result;
import code.ponfee.commons.model.ResultCode;
import code.ponfee.commons.model.SearchAfter;
import code.ponfee.commons.model.SortField;
import code.ponfee.commons.reflect.BeanMaps;
import code.ponfee.commons.util.ObjectUtils;
import code.ponfee.es.bulk.configuration.BulkProcessorConfiguration;
import code.ponfee.es.mapping.ElasticSearchMapping;

/**
 * ElasticSearch Client
 * 
 * {@link org.elasticsearch.client.transport.TransportClient}：
 *   轻量级的Client，使用Netty线程池，Socket连接到ES集群。本身不加入到集群，只作为请求的处理
 * {@link org.elasticsearch.client.node.NodeClient}          ：
 *   客户端节点本身也是ES节点，加入到集群，和其他ElasticSearch节点一样，频繁的开启和关闭这类Node Clients会在集群中产生“噪音”
 * 
 * @author Ponfee
 */
public class ElasticSearchClient implements DisposableBean {

    public static final String DOCUMENT_ID = "_id";

    static final TimeValue SCROLL_TIMEOUT = TimeValue.timeValueSeconds(120); // 2 minutes

    private static final int SCROLL_SIZE = 10000; // 默认滚动数据量
    private static final BulkProcessorConfiguration BULK_PROCESSOR = new BulkProcessorConfiguration();

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

    private final TransportClient client; // ES集群客户端
    private final BeanMaps convertor;

    public ElasticSearchClient(String clusterName, String clusterNodes) {
        this(clusterName, clusterNodes, BeanMaps.PROPS);
    }

    /**
     * @param clusterName  集群名称：es-cluster
     * @param clusterNodes 集群节点列表：ip1:port1,ip2:port2
     * @param BeanMaps     the BeanMaps, convert elasticsearch result source map to target bean
     */
    public ElasticSearchClient(String clusterName, String clusterNodes, BeanMaps convertor) {
        this.client = createClient(clusterName, clusterNodes);
        this.convertor = convertor;
    }

    /**
     * <code>
     *  try (Client client = createClient(clusterName, clusterNodes)) {
     *    // do something...
     *  }
     * </code>
     * @param clusterName
     * @param clusterNodes
     * @return
     */
    public static TransportClient createClient(String clusterName, String clusterNodes) {
        Settings settings = Settings.builder()
            .put("cluster.name", clusterName)
            .put("client.transport.sniff", true) // 启动嗅探功能，这样只需要指定集群中的某一个节点(不一定是主节点)，
                                                 // 然后会加载集群中的其他节点，这样只要程序不停即使此节点宕机仍然可以连接到其他节点
            .put("client.transport.ignore_cluster_name", false)
            //.put("client.transport.ping_timeout", "15s")
            //.put("client.transport.nodes_sampler_interval", "5s")
            .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        logger.info("Init ElasticSearch Client Start: {}, {}", clusterName, clusterNodes);
        Stream.of(split(clusterNodes, ",")).forEach(clusterNode -> {
            try {
                String[] nodeInfos = clusterNode.split(":", 2);
                InetAddress hostName = InetAddress.getByName(nodeInfos[0]);
                int port = Integer.parseInt(nodeInfos[1]);
                client.addTransportAddress(new InetSocketTransportAddress(hostName, port));
            } catch (UnknownHostException e) {
                logger.error("Cannot Connect ElasticSearch Node: {}, {}", clusterName, clusterNode, e);
            }
        });

        client.connectedNodes().stream().forEach(
            node -> logger.info("Connected ElasticSearch Node: {}", node.getHostAddress())
        );
        logger.info("Init ElasticSearch Client End: {}, {}", clusterName, clusterNodes);
        return client;
    }

    public TransportClient client() {
        return client;
    }

    // ---------------------------------------------------------------------------------------------create index
    /**
     * 创建空索引： 默认setting，无mapping
     * 
     * @param index
     * @return
     */
    public boolean createIndex(String index) {
        return indicesAdminClient().prepareCreate(index).get().isAcknowledged();
    }

    /**
     * 创建索引，默认setting，设置type的mapping
     * @param index
     * @param type
     * @param mappingJson
     * @return
     */
    public boolean createIndex(String index, String type, String mappingJson) {
        return createIndex(index, type, mappingJson, null);
    }

    /**
     * 创建索引，指定setting，设置type的mapping
     * 
     * @param index
     * @param type
     * @param mappingJson
     * @param settingJson
     * @return
     */
    public boolean createIndex(String index, String type, String mappingJson, String settingJson) {
        return createIndex(index, type, mappingJson, Function.identity(), settingJson, Function.identity());
    }

    /**
     * 创建索引，指定setting，设置type的mapping
     * 
     * @param index
     * @param settings
     * @param type
     * @param mapping
     * @return
     */
    public <T, E> boolean createIndex(String index, String type, 
                                      T mapping, Function<T, String> mappingJsonMapper,
                                      E setting, Function<E, String> settingJsonMapper) {
        // Settings settings = Settings.builder().put("index.number_of_shards", 3)
        //                                       .put("index.number_of_replicas", 2).build();
        CreateIndexRequestBuilder req = indicesAdminClient().prepareCreate(index);
        if (setting != null) {
            req.setSettings(settingJsonMapper.apply(setting), JSON);
        }
        return req.addMapping(type, mappingJsonMapper.apply(mapping), JSON).get().isAcknowledged();
    }

    // ---------------------------------------------------------------------------------------------put mapping
    /**
     * XContentBuilder mapping = XContentFactory.jsonBuilder()
     * .startObject() // {
     *   .startObject("user_mapping") // "user":{ // type name
     *     .startObject("_ttl") // "_ttl":{ //给记录增加了失效时间，ttl的使用地方如在分布式下（如web系统用户登录状态的维护）
     *       .field("enabled", true) // 默认的false的  
     *       .field("default", "5m") // 默认的失效时间：d/h/m/s（/小时/分钟/秒）  
     *       .field("store", "yes")
     *       .field("index", "not_analyzed")
     *     .endObject() // }
     *     .startObject("_timestamp") // 表示添加一条索引记录后自动给该记录增加个时间字段（记录的创建时间），供搜索使用
     *       .field("enabled", true)
     *       .field("store", "no")
     *       .field("index", "not_analyzed")
     *     .endObject() // }
     *     .startObject("properties") // properties下定义的name为自定义字段，相当于数据库中的表字段 
     *       .startObject("@timestamp").field("type", "long").endObject()
     *       .startObject("name").field("type", "string").field("store", "yes").endObject()
     *       .startObject("home").field("type", "string").field("index", "not_analyzed").endObject()
     *       .startObject("now_home").field("type", "string").field("index", "not_analyzed").endObject()
     *       .startObject("height").field("type", "double").endObject()
     *       .startObject("age").field("type", "integer").endObject()
     *       .startObject("birthday").field("type", "date").field("format", "yyyy-MM-dd").endObject()
     *       .startObject("isRealMen").field("type", "boolean").endObject()
     *       .startObject("location").field("lat", "double").field("lon", "double").endObject()
     *     .endObject() // }
     *   .endObject() // }
     * .endObject(); // }
     * 
     * 创建类型，设置mapping
     * 
     * @param index
     * @param type
     * @param mappingJson  json格式的mapping
     */
    public boolean putMapping(String index, String type, String mappingJson) {
        // XContentFactory.xContentType(String)
        return putMapping(index, type, mappingJson, Function.identity());
    }

    public <T> boolean putMapping(String index, String type, T mappingSource, 
                                  @Nonnull Function<T, String> jsonMapper) {
        return indicesAdminClient()
           .preparePutMapping(index).setType(type)
           .setSource(jsonMapper.apply(mappingSource), JSON)
           .get().isAcknowledged();
    }

    /**
     * 创建mapping
     * 
     * @param indexName
     * @param esMapping
     * @return
     */
    public boolean putMapping(String indexName, ElasticSearchMapping esMapping) {
        XContentBuilder content = esMapping.getMapping();
        PutMappingRequest req = new PutMappingRequest(esMapping.getIndex())
            .type(esMapping.getType()).source(content, content.contentType());
        return indicesAdminClient().putMapping(req).actionGet().isAcknowledged();
    }

    /**
     * 删除索引
     * @param index
     */
    public boolean deleteIndex(String index) {
        return indicesAdminClient().prepareDelete(index).get().isAcknowledged();
    }

    /**
     * 关闭索引
     * @param index
     * @return
     */
    public boolean closeIndex(String index) {
        return indicesAdminClient().prepareClose(index).get().isAcknowledged();
    }

    /**
     * 打开索引
     * @param index
     * @return
     */
    public boolean openIndex(String index) {
        return indicesAdminClient().prepareOpen(index).get().isAcknowledged();
    }

    /**
     * 索引状态
     * @param index
     * @return
     */
    public String indexStats(String index) {
        return indicesAdminClient().prepareStats(index).all().get().toString();
    }

    /**
     * 更新设置
     * @param index
     * @param settings
     * @return
     */
    public boolean updateSettings(String index, Settings settings) {
        return indicesAdminClient().prepareUpdateSettings(index)
                                   .setSettings(settings)
                                   .get().isAcknowledged();
    }

    /**
     * 添加别名
     * @param alias
     * @param indices
     * @return  是否创建成功
     */
    public boolean addAlias(String alias, String... indices) {
        return indicesAdminClient().prepareAliases()
                                   .addAlias(indices, alias)
                                   .execute().isDone();
    }

    /**
     * 更换别名
     * @param newAlias
     * @param oldAliase
     * @param indices
     * @return
     */
    public boolean changeAlias(String newAlias, String[] oldAliase, String... indices) {
        IndicesAliasesRequestBuilder builder = indicesAdminClient().prepareAliases();
        builder.removeAlias(indices, oldAliase).addAlias(indices, newAlias);
        return builder.execute().isDone();
    }

    /**
     * 删除别名
     * @param aliase
     * @param indices
     * @return
     */
    public boolean removeAlias(String[] aliase, String... indices) {
        return indicesAdminClient().prepareAliases()
                                   .removeAlias(indices, aliase)
                                   .execute().isDone();
    }

    /**
     * 判断索引是否存在
     * @param indices
     * @return
     */
    public boolean isIndicesExists(String... indices) {
        return indicesAdminClient().prepareExists(indices).get().isExists();
    }

    /**
     * 判断别名是否存在
     * @param aliases
     * @return
     */
    public boolean isAliasExists(String... aliases) {
        return indicesAdminClient().prepareAliasesExist(aliases).get().isExists();
    }

    /**
     * 判断类型是否存在
     * @param indices
     * @param types
     * @return
     */
    public boolean isTypesExists(String[] indices, String... types) {
        return indicesAdminClient().prepareTypesExists(indices)
                                   .setTypes(types)
                                   .get().isExists();
    }

    // -----------------------------------------------------------------------------add document without id
    public String addDoc(String index, String type, String json) {
        return addDoc(index, type, json, Function.identity());
    }

    /**
     * 添加文档，不指定id（POST）
     * 
     * @param index
     * @param type
     * @param object
     * @param jsonMapper
     */
    public <T> String addDoc(String index, String type, T object, 
                             @Nonnull Function<T, String> jsonMapper) {
        return client.prepareIndex(index, type)
                     .setSource(jsonMapper.apply(object), JSON)
                     .get().getId();
    }

    // -----------------------------------------------------------------------------add document specify id
    public String addDoc(String index, String type, String id, String json) {
        return addDoc(index, type, id, json, Function.identity());
    }

    /**
     * 添加文档，指定id（PUT）
     * 
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     指定id
     * @param object 要增加的source
     * @param jsonMapper 
     * @return
     */
    public <T> String addDoc(String index, String type, String id, T object, 
                             @Nonnull Function<T, String> jsonMapper) {
        return client.prepareIndex(index, type, id)
                     .setSource(jsonMapper.apply(object), JSON)
                     .get().getId();
    }

    // -----------------------------------------------------------------------------batch add document
    public Result<Void> addDocs(String index, String type, List<String> list) {
        return addDocs(index, type, list, Function.identity(), null);
    }

    /**
     * Batch add documents with Bulk
     *   if idMapper is {@code null} then non specify id with POST
     *   else specify id with PUT
     * 
     * @param index the index
     * @param type  the type
     * @param list  the data list
     * @param jsonMapper the json mapper
     * @param idMapper  the id mapper
     * @return a Result of batch
     */
    public <T> Result<Void> addDocs(String index, String type, List<T> list, 
                                    @Nonnull Function<T, String> jsonMapper, 
                                    @Nullable Function<T, String> idMapper) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        list.stream().forEach(x -> {
            IndexRequestBuilder builder = client.prepareIndex(index, type);
            if (idMapper != null) {
                builder.setId(idMapper.apply(x));
            }
            bulkRequest.add(builder.setSource(jsonMapper.apply(x), JSON)); // id尽量为物理表的主键
        });
        BulkResponse resp = bulkRequest.get();
        if (resp.hasFailures()) {
            return Result.failure(ResultCode.SERVER_ERROR, resp.buildFailureMessage());
        } else {
            return Result.success();
        }
    }

    // -----------------------------------------------------------------------------update document
    public boolean updDoc(String index, String type, String id, String json) {
        return updDoc(index, type, id, json, Function.identity());
    }

    /**
     * 指定ID更新文档
     * 
     * @param index
     * @param type
     * @param id
     * @param object
     * @param jsonMapper
     */
    public <T> boolean updDoc(String index, String type, String id, T object, 
                              @Nonnull Function<T, String> jsonMapper) {
        try {
            UpdateResponse resp = client.update(
                new UpdateRequest(index, type, id).doc(jsonMapper.apply(object), JSON)
            ).get();
            return resp.getResult() == DocWriteResponse.Result.UPDATED;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Update doc occur error, index:{}, type:{}, id:{}, object:{}", index, type, id, object, e);
            return false;
        }
    }

    // -----------------------------------------------------------------------------bulk update documents
    /**
     * Bulk update documents
     * 
     * @param index
     * @param type
     * @param list
     * @param jsonMapper
     * @param idMapper
     * @return
     */
    public <T> Result<Void> updDocs(String index, String type, List<T> list, 
                                    @Nonnull Function<T, String> jsonMapper,
                                    @Nonnull Function<T, String> idMapper) {
        BulkRequestBuilder bulkReq = client.prepareBulk();
        list.forEach(x -> {
            bulkReq.add(
                new UpdateRequest(index, type, idMapper.apply(x)).doc(jsonMapper.apply(x), JSON)
            );
        });
        BulkResponse resp = bulkReq.get();
        if (resp.hasFailures()) {
            return Result.failure(ResultCode.SERVER_ERROR, resp.buildFailureMessage());
        } else {
            return Result.success();
        }
    }

    // -----------------------------------------------------------------------------bulk add or update documents
    /**
     * Update or insert(if the document does not exists) documents
     * 
     * @param index the es index
     * @param type  the es type
     * @param list  source list
     * @param jsonMapper
     * @param idMapper
     * @return update result
     */
    public <T> Result<Void> upsertDocs(String index, String type, List<T> list,
                                       @Nonnull Function<T, String> jsonMapper,
                                       @Nonnull Function<T, String> idMapper) {
        BulkRequestBuilder bulkReq = client.prepareBulk();
        list.forEach(x -> {
            String json = jsonMapper.apply(x), id = idMapper.apply(x);
            bulkReq.add(
                new UpdateRequest(index, type, id).doc(json, JSON).upsert(
                    new IndexRequest(index, type, id).source(json, JSON)
                )
            );
            // client.prepareUpdate(index, type, id).setDoc(json, JSON).setDocAsUpsert(true);
        });

        BulkResponse resp = bulkReq.get();
        if (resp.hasFailures()) {
            return Result.failure(ResultCode.SERVER_ERROR, resp.buildFailureMessage());
        } else {
            return Result.success();
        }
    }

    // ------------------------------------------------------------------------------------------del doc
    /**
     * 删除文档
     * @param index
     * @param type
     * @param id
     */
    public boolean delDoc(String index, String type, String id) {
        DeleteResponse resp = client.prepareDelete(index, type, id).get();
        return resp.getResult() == DocWriteResponse.Result.DELETED;
    }

    // ------------------------------------------------------------------------------------------get doc
    @SuppressWarnings("unchecked")
    public Map<String, Object> getDoc(String index, String type, String id) {
        GetResponse response = client.prepareGet(index, type, id).get();
        return convertFromMap(response.getSource(), response.getId(), Map.class);
    }

    /**
     * 获取文档
     * @param index
     * @param type
     * @param clazz  document entity type
     * @param id     document id
     * @return return the documens of specific id
     */
    public <T> T getDoc(String index, String type, Class<T> clazz, String id) {
        GetResponse response = client.prepareGet(index, type, id).get();
        return convertFromMap(response.getSource(), response.getId(), clazz);
    }

    public List<Map<String, Object>> getDocs(String index, String type, String[] ids) {
        return getDocs(index, type, ids, null);
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getDocs(String index, String type, String[] ids, String[] fields) {
        return (List<Map<String, Object>>) (List<?>) getDocs(index, type, Map.class, ids, fields);
    }

    public <T> List<T> getDocs(String index, String type, Class<T> clazz, String[] ids) {
        return getDocs(index, type, clazz, ids, null);
    }

    /**
     * 批量获取（mget）
     * 
     * @param index
     * @param type
     * @param clazz
     * @param ids
     * @return return the documents of specific id array
     */
    public <T> List<T> getDocs(String index, String type, Class<T> clazz, String[] ids, String[] fields) {
        MultiGetRequestBuilder reqBuilder = client.prepareMultiGet();
        if (ArrayUtils.isEmpty(fields)) {
            for (String id : ids) {
                reqBuilder.add(index, type, id);
            }
        } else {
            for (String id : ids) {
                reqBuilder.add(new Item(index, type, id).fetchSourceContext(new FetchSourceContext(true, fields, null)));
            }
        }
        MultiGetResponse multiResp = reqBuilder.get();
        List<T> result = new ArrayList<>(multiResp.getResponses().length);
        for (MultiGetItemResponse itemResp : multiResp) {
            GetResponse response = itemResp.getResponse();
            if (response.isExists()) {
                result.add(convertFromMap(response.getSource(), response.getId(), clazz));
            }
        }
        return result;
    }

    // -----------------------------------------------------------------------------bulk processor add document
    public <T> boolean addBulk(String index, String type, T object) {
        return addBulk(index, type, Collections.singletonList(object));
    }

    public <T> boolean addBulk(String index, String type, List<T> data) {
        return addBulk(index, type, data, BULK_PROCESSOR, Jsons::toJson, null);
    }

    public <T> boolean addBulk(String index, String type, List<T> data, 
                               @Nonnull Function<T, String> jsonMapper,
                               @Nullable Function<T, String> idMapper) {
        return addBulk(index, type, data, BULK_PROCESSOR, jsonMapper, idMapper);
    }

    /**
     * 批量添加文档
     * 
     * @param index
     * @param type
     * @param data
     * @param config     BulkProcessorConfiguration config
     * @param jsonMapper object to json
     * @param idMapper   get object id, if {@code null} then non specify id to add
     */
    public <T> boolean addBulk(String index, String type, List<T> data, 
                               BulkProcessorConfiguration config, 
                               @Nonnull Function<T, String> jsonMapper,
                               @Nullable Function<T, String> idMapper) {
        BulkProcessor bulkProcessor = config.build(client);
        data.stream().filter(
            Objects::nonNull
        ).forEach(x -> {
            IndexRequestBuilder builder = client.prepareIndex(index, type);
            if (idMapper != null) {
                builder.setId(idMapper.apply(x));
            }
            bulkProcessor.add(builder.setSource(jsonMapper.apply(x), JSON).request());
        });
        bulkProcessor.flush();

        try {
            return bulkProcessor.awaitClose(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("bulk process error", e);
            return false;
        }
    }

    public <T> boolean updBulk(String index, String type, List<T> data, 
                               @Nonnull Function<T, String> jsonMapper,
                               @Nonnull Function<T, String> idMapper) {
        return updBulk(index, type, data, BULK_PROCESSOR, jsonMapper, idMapper);
    }

    /**
     * 批量更新文档
     * 
     * @param index
     * @param type
     * @param data
     * @param config     BulkProcessorConfiguration config
     * @param jsonMapper object to json
     * @param idMapper   get object id
     */
    public <T> boolean updBulk(String index, String type, List<T> data, 
                               BulkProcessorConfiguration config, 
                               @Nonnull Function<T, String> jsonMapper,
                               @Nonnull Function<T, String> idMapper) {
        BulkProcessor bulkProcessor = config.build(client);
        data.stream().filter(
            Objects::nonNull
        ).forEach(x -> {
            bulkProcessor.add(
                client.prepareUpdate(index, type, idMapper.apply(x))
                .setDoc(jsonMapper.apply(x), JSON).request()
            );
        });
        bulkProcessor.flush();

        try {
            return bulkProcessor.awaitClose(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("bulk process error", e);
            return false;
        }
    }

    public <T> boolean upsertBulk(String index, String type, List<T> data,
                                  @Nonnull Function<T, String> jsonMapper,
                                  @Nonnull Function<T, String> idMapper) {
        return upsertBulk(index, type, data, BULK_PROCESSOR, jsonMapper, idMapper);
    }

    /**
     * 批量更新或新增文档
     * 
     * @param index
     * @param type
     * @param data
     * @param config     BulkProcessorConfiguration config
     * @param jsonMapper object to json
     * @param idMapper   get object id
     */
    public <T> boolean upsertBulk(String index, String type, List<T> data, 
                                  BulkProcessorConfiguration config, 
                                  @Nonnull Function<T, String> jsonMapper,
                                  @Nonnull Function<T, String> idMapper) {
        BulkProcessor bulkProcessor = config.build(client);
        data.stream().filter(
            Objects::nonNull
        ).forEach(x -> {
            String json = jsonMapper.apply(x), id = idMapper.apply(x);
            bulkProcessor.add(
                new UpdateRequest(index, type, id).doc(json, JSON).upsert(
                    new IndexRequest(index, type, id).source(json, JSON)
                )
            );
        });
        bulkProcessor.flush();

        try {
            return bulkProcessor.awaitClose(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("bulk process error", e);
            return false;
        }
    }

    // --------------------------------------------------------------------------------prepareSearch
    /**
     * 获取搜索请求对象
     * @param indexName
     * @param typeName
     * @return SearchRequestBuilder
     */
    public SearchRequestBuilder prepareSearch(String indexName, String typeName) {
        return client.prepareSearch(indexName).setTypes(typeName);
    }

    public SearchRequestBuilder prepareSearch(String indexName, String... typeNames) {
        return client.prepareSearch(indexName).setTypes(typeNames);
    }

    public SearchRequestBuilder prepareSearch(String[] indexNames, String typeName) {
        return client.prepareSearch(indexNames).setTypes(typeName);
    }

    public SearchRequestBuilder prepareSearch(String[] indexNames, String[] typeNames) {
        return client.prepareSearch(indexNames).setTypes(typeNames);
    }

    public MultiSearchRequestBuilder prepareMultiSearch() {
        return this.client.prepareMultiSearch();
    }

    // ----------------------------------------------------------------------------------------分页搜索
    /**
     * 深分页查询（针对用户实时查询）
     * 
     * @param query
     * @param pageNo
     * @param pageSize
     * @return  page result and map of row record
     * 
     * @deprecated 分页搜索对服务器压力大（超过index.max_result_window会报错）, 
     *             Use {@link #searchAfter(SearchRequestBuilder, int, SearchAfter...)} Or {@link #scrollSearch(ESQueryBuilder, int, ScrollSearchCallback)} instead
     */
    @Deprecated
    public Page<Map<String, Object>> paginationSearch(ESQueryBuilder query, int pageNo, int pageSize) {
        return this.paginationSearch(query, pageNo, pageSize, Map.class).copy();
    }

    /**
     * 深分页查询（针对用户实时查询）
     * 
     * @param query      查询条件
     * @param pageNo     页码
     * @param pageSize   页大小
     * @param clazz      返回的行数据类型
     * @return
     * 
     * @deprecated Use {@link #searchAfter(SearchRequestBuilder, int, SearchAfter...)} Or {@link #scrollSearch(ESQueryBuilder, int, ScrollSearchCallback)} instead
     */
    @Deprecated
    public <T> Page<T> paginationSearch(ESQueryBuilder query, int pageNo, 
                                        int pageSize, Class<T> clazz) {
        int from = (pageNo - 1) * pageSize;
        return buildPage(query.pagination(client, from, pageSize), from, pageNo, pageSize, clazz);
    }

    /**
     * 深分页查询（针对用户实时查询）
     * @param search    SearchRequestBuilder
     * @param pageNo    页码
     * @param pageSize  页大小
     * @return
     * 
     * @deprecated Use {@link #searchAfter(SearchRequestBuilder, int, SearchAfter...)} Or {@link #scrollSearch(ESQueryBuilder, int, ScrollSearchCallback)} instead
     */
    @Deprecated
    public Page<Map<String, Object>> paginationSearch(SearchRequestBuilder search, 
                                                      int pageNo, int pageSize) {
        return this.paginationSearch(search, pageNo, pageSize, Map.class).copy();
    }

    /**
     * 深分页搜索
     * @param search    SearchRequestBuilder
     * @param pageNo    page number
     * @param pageSize  size of page
     * @param clazz     row object type
     * @return page result
     * 
     * @deprecated Use {@link #searchAfter(SearchRequestBuilder, int, SearchAfter...)} Or {@link #scrollSearch(ESQueryBuilder, int, ScrollSearchCallback)} instead
     */
    @Deprecated
    public <T> Page<T> paginationSearch(SearchRequestBuilder search, int pageNo, 
                                        int pageSize, Class<T> clazz) {
        int from = (pageNo - 1) * pageSize;
        search.setSearchType(SearchType.DFS_QUERY_THEN_FETCH) // 深度分布
              .setFrom(from).setSize(pageSize).setExplain(false);
        return buildPage(search.get(), from, pageNo, pageSize, clazz);
    }

    // ----------------------------------------------------------------------------排名搜索
    /**
     * 排名搜索
     * @param search
     * @param ranking
     * @return
     */
    public List<Map<String, Object>> rankingSearch(SearchRequestBuilder search, int ranking) {
        return paginationSearch(search, 1, ranking).getRows();
    }

    public <T> List<T> rankingSearch(SearchRequestBuilder search, int ranking, Class<T> clazz) {
        return paginationSearch(search, 1, ranking, clazz).getRows();
    }

    public List<Map<String, Object>> rankingSearch(ESQueryBuilder query, int ranking) {
        return paginationSearch(query, 1, ranking).getRows();
    }

    public <T> List<T> rankingSearch(ESQueryBuilder query, int ranking, Class<T> clazz) {
        return paginationSearch(query, 1, ranking, clazz).getRows();
    }

    // --------------------------------------------------------------------------------------顶部搜索
    public Map<String, Object> topSearch(SearchRequestBuilder query) {
        List<Map<String, Object>> list = rankingSearch(query, 1);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    public <T> T topSearch(SearchRequestBuilder query, Class<T> clazz) {
        List<T> list = rankingSearch(query, 1, clazz);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    public Map<String, Object> topSearch(ESQueryBuilder query) {
        List<Map<String, Object>> list = rankingSearch(query, 1);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    public <T> T topSearch(ESQueryBuilder query, Class<T> clazz) {
        List<T> list = rankingSearch(query, 1, clazz);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    // --------------------------------------------------------------------------------------滚动搜索
    /**
     * 滚动搜索（游标查询，针对大数据量甚至是全表查询时使用）
     * 符合条件的数据全部查询（不分页场景使用）
     * 分页查询请用paginationSearch {@link #paginationSearch(ESQueryBuilder, int, int)}
     * @param query       查询条件
     * @param scrollSize  每次滚动的数据量大小
     * @param callback    回调处理量
     */
    public void scrollSearch(ESQueryBuilder query, int scrollSize, 
                             ScrollSearchCallback callback) {
        this.scrollSearch(query.scroll(client, scrollSize), scrollSize, callback);
    }

    /**
     * 滚动搜索（游标查询，针对大数据量甚至是全表查询时使用）
     * 符合条件的数据全部查询（不分页场景使用）
     * @param search
     * @param scrollSize
     * @param callback
     */
    public void scrollSearch(SearchRequestBuilder search, int scrollSize, 
                             ScrollSearchCallback callback) {
        SearchResponse scrollResp = search.setSize(scrollSize).setScroll(SCROLL_TIMEOUT).get();
        this.scrollSearch(scrollResp, scrollSize, callback);
    }

    // ----------------------------------------------------------------------------------------全部搜索
    @SuppressWarnings("rawtypes")
    public List<Map> fullSearch(ESQueryBuilder query) {
        return fullSearch(query, Map.class, SCROLL_SIZE);
    }

    public <T> List<T> fullSearch(ESQueryBuilder query, Class<T> clazz) {
        return fullSearch(query, clazz, SCROLL_SIZE);
    }

    /**
     * 查询全部数据
     * @param query
     * @param clazz
     * @return
     */
    public <T> List<T> fullSearch(ESQueryBuilder query, Class<T> clazz, int eachScrollSize) {
        SearchResponse scrollResp = query.scroll(client, eachScrollSize);
        List<T> result = new ArrayList<>((int) scrollResp.getHits().getTotalHits());
        this.scrollSearch(
            scrollResp, eachScrollSize, 
            (searchHits, totalRecords, totalPages, pageNo) -> {
                for (SearchHit hit : searchHits.getHits()) {
                    result.add(convertFromMap(hit.getSourceAsMap(), hit.getId(), clazz));
                }
            }
        );
        return result;
    }

    @SuppressWarnings("rawtypes")
    public List<Map> fullSearch(SearchRequestBuilder search) {
        return fullSearch(search, Map.class);
    }

    public <T> List<T> fullSearch(SearchRequestBuilder search, Class<T> clazz) {
        return fullSearch(search, clazz, SCROLL_SIZE);
    }

    /**
     * 查询全部数据
     * @param search
     * @param clazz
     * @return
     */
    public <T> List<T> fullSearch(SearchRequestBuilder search, Class<T> clazz, int eachScrollSize) {
        SearchResponse scrollResp = search.setSize(eachScrollSize).setScroll(SCROLL_TIMEOUT).get();
        List<T> result = new ArrayList<>((int) scrollResp.getHits().getTotalHits());
        this.scrollSearch(scrollResp, eachScrollSize, (searchHits, totalRecords, totalPages, pageNo) -> {
            for (SearchHit hit : searchHits.getHits()) {
                result.add(convertFromMap(hit.getSourceAsMap(), hit.getId(), clazz));
            }
        });
        return result;
    }

    // ----------------------------------------------------------------------------------------search-after
    @SuppressWarnings("unchecked")
    public <E> List<Map<String, Object>> searchAfter(SearchRequestBuilder search, int size, 
                                                     SearchAfter<E>... searchAfters) {
        List<?> result = searchAfter(search, size, Map.class, searchAfters);
        return (List<Map<String, Object>>) result;
    }

    @SuppressWarnings("unchecked")
    public <T, E> List<T> searchAfter(SearchRequestBuilder search, int size, 
                                      Class<T> clazz, SearchAfter<E>... searchAfters) {
        Object[] values = new Object[searchAfters.length];
        int i = 0;
        for (SearchAfter<E> sa : searchAfters) {
            SortField sf = sa.getSortField();
            values[i++] = sa.getValue();
            search.addSort(sf.getField(), SortOrder.fromString(sf.getSortOrder().name()));
        }
        search.searchAfter(values).setSize(size);

        SearchResponse resp = search.get();
        SearchHit[] hits = resp.getHits().getHits();
        if (ArrayUtils.isEmpty(hits)) {
            return Collections.emptyList();
        } else {
            List<T> result = new ArrayList<>(hits.length);
            for (SearchHit hit : hits) {
                result.add(convertFromMap(hit.getSourceAsMap(), hit.getId(), clazz));
            }
            return result;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <E> void searchEnd(Supplier<SearchRequestBuilder> search, int size, 
                                 Consumer<List<Map>> consumer, 
                                 SearchAfter<E>... searchStarts) {
        searchEnd(search, size, Map.class, consumer, searchStarts);
    }

    @SuppressWarnings({ "unchecked" })
    public <T, E> void searchEnd(Supplier<SearchRequestBuilder> search, int size, Class<T> clazz, 
                                 Consumer<List<T>> consumer, SearchAfter<E>... searchStarts) {
        List<T> each;
        do {
            each = searchAfter(search.get(), size, clazz, searchStarts);
            if (!each.isEmpty()) {
                T endRow = each.get(each.size() - 1);
                for (int i = 0; i < searchStarts.length; i++) {
                    SearchAfter<E> sa = searchStarts[i];
                    searchStarts[i] = sa.copy(
                        (E) ObjectUtils.getValue(endRow, sa.getSortField().getField())
                    );
                }
                consumer.accept(each);
            }
        } while (each.size() == size);
    }

    // ----------------------------------------------------------------------------------------聚合汇总
    public Aggregations aggregationSearch(ESQueryBuilder query) {
        return query.aggregation(client);
    }

    /**
     * 聚合汇总查询
     * @param search
     * @return
     */
    public Aggregations aggregationSearch(SearchRequestBuilder search) {
        return search.setSize(0).get().getAggregations();
    }

    // ----------------------------------------------------------------------------------------其它
    /**
     * 销毁:关闭连接，释放资源
     */
    @Override
    public void destroy() {
        logger.info("Elasticsearch client closing...");
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                logger.error("closing elasticsearch client error.", e);
            }
        }
        logger.info("Elasticsearch client was closed.");
    }

    /**
     * 判断集群是否有可用节点
     * @return true or false
     */
    public boolean isAvailable() {
        return client != null && client.connectedNodes().size() > 0;
    }

    // -------------------------------------------------------------------------------scripts
    /**
     * 使用脚本更新文档：painless，groovy
     * 
     * @param index
     * @param type
     * @param id
     * @param idOrCode
     * @param params
     */
    public void updateByGroovy(String index, String type, String id, 
                               String idOrCode, Map<String, Object> params) {
        UpdateRequestBuilder req = client.prepareUpdate(index, type, id);
        req.setScript(new Script(ScriptType.INLINE, "groovy", idOrCode, params)).get();
    }

    // ------------------------------------------------------------------------private methods
    /**
     * 获取索引管理客户端
     * @return IndicesAdminClient
     */
    private IndicesAdminClient indicesAdminClient() {
        return client.admin().indices();
    }

    /**
     * 构建返回搜索结果页
     * @param searchResp
     * @param from
     * @param pageNo
     * @param pageSize
     * @param clazz
     * @return
     */
    private <T> Page<T> buildPage(SearchResponse searchResp, int from, 
                                  int pageNo, int pageSize, Class<T> clazz) {
        SearchHits hits = searchResp.getHits();
        long total = hits.getTotalHits();
        List<T> result = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits) {
            result.add(convertFromMap(hit.getSourceAsMap(), hit.getId(), clazz));
        }

        Page<T> page = Page.of(result);
        page.setTotal(total);
        page.setPages(PageHandler.computeTotalPages(total, pageSize)); // 总页数
        page.setPageNum(pageNo);
        page.setPageSize(pageSize);
        page.setSize(result.size());
        page.setStartRow(from);
        page.setEndRow(from + result.size() - 1);
        page.setFirstPage(pageNo == 1);
        page.setLastPage(pageNo == page.getPages());
        page.setHasNextPage(pageNo > 1);
        page.setHasNextPage(pageNo < page.getPages());
        page.setPrePage(pageNo - 1);
        page.setNextPage(pageNo + 1);
        return page;
    }

    /**
     * 滚动搜索结果
     * @param scrollResp
     * @param scrollSize
     * @param callback
     */
    private void scrollSearch(SearchResponse scrollResp, int scrollSize, 
                              ScrollSearchCallback callback) {
        try {
            SearchHits searchHits = scrollResp.getHits();
            long totalRecords = searchHits.getTotalHits(); // 总记录数
            int totalPages = (int) ((totalRecords + scrollSize - 1) / scrollSize); // 总页数

            if (logger.isInfoEnabled()) {
                logger.info("scroll search: {} total[{}-{}]", 
                            ObjectUtils.getStackTrace(4), totalPages, totalRecords);
            }

            if (totalRecords == 0) {
                callback.noResult();
            } else {
                int pageNo = 1;
                do {
                    callback.nextPage(searchHits, totalRecords, totalPages, pageNo++);
                    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                                       .setScroll(SCROLL_TIMEOUT).get();
                } while ((searchHits = scrollResp.getHits()).getHits().length != 0);
            }
        } finally {
            client.prepareClearScroll().addScrollId(scrollResp.getScrollId()).get(); // 清除
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T convertFromMap(Map<String, Object> data, String id, Class<T> clazz) {
        if (data == null) {
            return null;
        } 

        data.put(DOCUMENT_ID, id);

        return clazz.isAssignableFrom(data.getClass()) ? (T) data : convertor.toBean(data, clazz);
    }

}
