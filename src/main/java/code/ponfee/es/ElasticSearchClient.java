package code.ponfee.es;

import static org.apache.commons.lang3.StringUtils.split;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import code.ponfee.commons.json.Jsons;
import code.ponfee.commons.model.Page;
import code.ponfee.commons.model.PageHandler;
import code.ponfee.commons.model.Result;
import code.ponfee.commons.model.ResultCode;
import code.ponfee.commons.util.ObjectUtils;
import code.ponfee.es.bulk.configuration.BulkProcessorConfiguration;
import code.ponfee.es.mapping.IElasticSearchMapping;

/**
 * ElasticSearch Client
 * 
 * @author fupf
 */
public class ElasticSearchClient implements DisposableBean {

    static final TimeValue SCROLL_TIMEOUT = TimeValue.timeValueSeconds(120); // 2 minutes
    private static final int SCROLL_SIZE = 10000; // 默认滚动数据量

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

    private final TransportClient client; // ES集群客户端

    /**
     * @param clusterName  集群名称：es-cluster
     * @param clusterNodes 集群节点列表：ip1:port1,ip2:port2
     */
    public ElasticSearchClient(String clusterName, String clusterNodes) {
        client = createClient(clusterName, clusterNodes);
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
                                    .put("client.transport.sniff", true)
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

    /**
     * 创建空索引： 默认setting，无mapping
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
     * @param mapping
     * @return
     */
    public boolean createIndex(String index, String type, String mapping) {
        return createIndex(index, null, type, mapping);
    }

    /**
     * 创建索引，指定setting，设置type的mapping
     * 
     * @param index
     * @param settings
     * @param type
     * @param mapping
     * @param contentType
     * @return
     */
    public boolean createIndex(String index, String settings, String type, 
                               String mapping, XContentType contentType) {
        CreateIndexRequestBuilder req = indicesAdminClient().prepareCreate(index);
        if (settings != null) {
            req.setSettings(settings, contentType);
        }
        return req.addMapping(type, mapping, contentType).get().isAcknowledged();
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
    public boolean createIndex(String index, String settings, String type, String mapping) {
        // Settings settings = Settings.builder().put("index.number_of_shards", 3)
        //                                       .put("index.number_of_replicas", 2).build();
        XContentType contentType = XContentFactory.xContentType(settings);
        CreateIndexRequestBuilder req = indicesAdminClient().prepareCreate(index);
        if (settings != null) {
            req.setSettings(settings, contentType);
        }
        return req.addMapping(type, mapping, contentType).get().isAcknowledged();
    }

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
     * @param index
     * @param type
     * @param mapping  json格式的mapping
     */
    public boolean putMapping(String index, String type, String mapping) {
        XContentType contentType = XContentFactory.xContentType(mapping);
        /*try {
            PutMappingRequest mappingRequest = Requests.putMappingRequest(index).type(type);
            mappingRequest.source(mapping, contentType);
            return indicesAdminClient().putMapping(mappingRequest).actionGet().isAcknowledged(); // 创建索引结构
        } catch (Exception e) {
            logger.error("put mapping error: {} {} {}", index, type, mapping);
            indicesAdminClient().prepareDelete(index);
            return false;
        }*/
        return putMapping(index, type, mapping, contentType);
    }

    public boolean putMapping(String index, String type, String mapping, 
                              XContentType contentType) {
        return indicesAdminClient().preparePutMapping(index)
                                   .setType(type)
                                   .setSource(mapping, contentType)
                                   .get().isAcknowledged();
    }

    /**
     * 创建mapping
     * @param indexName
     * @param esMapping
     * @return
     */
    public boolean putMapping(String indexName, IElasticSearchMapping esMapping) {
        XContentBuilder contentType = esMapping.getMapping();
        PutMappingRequest req = new PutMappingRequest(indexName).type(esMapping.getIndexType())
                                               .source(contentType, contentType.contentType());
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
    public boolean isTypesExists(String indices, String... types) {
        return indicesAdminClient().prepareTypesExists(indices)
                                   .setTypes(types)
                                   .get().isExists();
    }

    // --------------------------------------bulk processor---------------------------------------
    public <T> void bulkProcessor(String index, String type, T entity) {
        bulkProcessor(index, type, Collections.singletonList(entity));
    }

    public <T> void bulkProcessor(String index, String type, List<T> entities) {
        bulkProcessor(index, type, entities.stream());
    }

    /**
     * 批量创建索引
     * @param index
     * @param type
     * @param entities    批量请求的数据（JSON格式）
     * @param config      批处理配置
     */
    public <T> void bulkProcessor(String index, String type, Stream<T> entities, 
                                  BulkProcessorConfiguration config) {
        BulkProcessor bulkProcessor = config.build(client);
        entities.map(x -> Optional.ofNullable(Jsons.toBytes(x)))
                .filter(Optional::isPresent)
                .map(x -> client.prepareIndex()
                                 .setIndex(index).setType(type)
                                .setSource(x.get(), XContentType.JSON)
                                .request()
                ).forEach(bulkProcessor::add);
        bulkProcessor.flush();
        try {
            bulkProcessor.awaitClose(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("bulk process error", e);
        }
    }

    // --------------------------------------document---------------------------------------
    /**
     * 添加文档，不指定id（POST）
     * @param index
     * @param type
     * @param object
     */
    public String addDoc(String index, String type, Object object) {
        //return client.prepareIndex(index, type).setSource(object).get().getId();
        return client.prepareIndex(index, type)
                     .setSource(object, XContentType.JSON)
                     .get().getId();
    }

    /**
     * 添加文档，指定id（PUT）
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     指定id
     * @param object 要增加的source
     */
    public String addDoc(String index, String type, String id, Object object) {
        //return client.prepareIndex(index, type, id).setSource(object).get().getId();
        return client.prepareIndex(index, type, id)
                     .setSource(object, XContentType.JSON)
                     .get().getId();
    }

    /**
     * 批量添加，不指定id（POST）
     * @param index
     * @param type
     * @param list
     * @return result
     */
    public Result<Void> addDocs(String index, String type, List<Object> list) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        list.stream().forEach(
            map -> bulkRequest.add(client.prepareIndex(index, type).setSource(map))
        );
        BulkResponse resp = bulkRequest.get();

        if (resp.hasFailures()) {
            return Result.failure(ResultCode.SERVER_ERROR, resp.buildFailureMessage());
        } else {
            return Result.success();
        }
    }

    /**
     * 添加文档，指定id（PUT）
     * 
     * @param index  索引
     * @param type   类型
     * @param map    文档数据：key为id，value为source
     * @return result
     */
    public Result<Void> addDocs(String index, String type, Map<String, Object> map) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        map.forEach((key, value) -> {
            bulkRequest.add(
                client.prepareIndex(index, type, key).setSource(value)
            );
        });
        BulkResponse resp = bulkRequest.get();
        if (resp.hasFailures()) {
            return Result.failure(ResultCode.SERVER_ERROR, resp.buildFailureMessage());
        } else {
            return Result.success();
        }
    }

    /**
     * 批量添加文档：map为source（其中含key为id的键，PUT）
     * 
     * @param index
     * @param type
     * @param list
     * @return result
     */
    public Result<Void> addDocsWithId(String index, String type, List<Map<String, Object>> list) {
        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (Map<String, Object> map : list) {
                XContentBuilder xcb = XContentFactory.jsonBuilder().startObject();
                for (Entry<String, Object> entry : map.entrySet()) {
                    if (!"id".equalsIgnoreCase(entry.getKey())) {
                        xcb.field(entry.getKey(), entry.getValue());
                    }
                }
                xcb.endObject();
                bulkRequest.add(
                    client.prepareIndex(index, type, Objects.toString(map.get("id")))
                          .setSource(xcb) // id尽量为物理表的主键
                );
            }
            BulkResponse resp = bulkRequest.get();
            if (resp.hasFailures()) {
                return Result.failure(ResultCode.SERVER_ERROR, resp.buildFailureMessage());
            } else {
                return Result.success();
            }
        } catch (IOException e) {
            logger.error("add docs error, index:{}, type:{}, object:{}", 
                         index, type, Jsons.toJson(list), e);
            return Result.failure(ResultCode.SERVER_ERROR);
        }
    }

    /**
     * 删除文档
     * @param index
     * @param type
     * @param id
     */
    public void delDoc(String index, String type, String id) {
        client.prepareDelete(index, type, id).get();
    }

    /**
     * 更新文档：key为field，value为field value
     * @param index
     * @param type
     * @param id
     * @param map
     */
    public void updDoc(String index, String type, String id, Map<String, Object> map) {
        //client.prepareIndex(index, type, id).setSource(json, XContentType.JSON).get();
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, type, id);
            XContentBuilder xcb = XContentFactory.jsonBuilder().startObject();
            for (Entry<String, Object> entry : map.entrySet()) {
                xcb.field(entry.getKey(), entry.getValue());
            }
            xcb.endObject();
            client.update(updateRequest.doc(xcb)).get();
        } catch (IOException | InterruptedException | ExecutionException e) {
            logger.error("update docs error, index:{}, type:{}, id:{}, object:{}", 
                         index, type, id, Jsons.toJson(map), e);
        }
    }

    /**
     * 批量更新文档
     * @param index
     * @param type
     * @param map    key为id，value为source
     * @return update result
     */
    public Result<Void> updDocs(String index, String type, Map<String, Object> map) {
        BulkRequestBuilder bulkReq = client.prepareBulk();
        for (Entry<String, Object> entry : map.entrySet()) {
            bulkReq.add(new UpdateRequest(index, type, entry.getKey()).doc(entry.getValue()));
        }
        BulkResponse resp = bulkReq.get();
        if (resp.hasFailures()) {
            return Result.failure(ResultCode.SERVER_ERROR, resp.buildFailureMessage());
        } else {
            return Result.success();
        }
    }

    /**
     * Batch update or insert(if the document does not exists) documents
     * 
     * @param index the es index
     * @param type  the es type
     * @param map   the map: key as id, value as source
     * @return update result
     */
    public Result<Void> upsertDocs(String index, String type, Map<String, Object> map) {
        BulkRequestBuilder bulkReq = client.prepareBulk();
        for (Entry<String, Object> entry : map.entrySet()) {
            bulkReq.add(
                new UpdateRequest(
                    index, type, entry.getKey()
                ).doc(
                    entry.getValue()
                ).upsert(
                    new IndexRequest(index, type, entry.getKey()).source(entry.getValue())
                )
            );
        }

        BulkResponse resp = bulkReq.get();
        if (resp.hasFailures()) {
            return Result.failure(ResultCode.SERVER_ERROR, resp.buildFailureMessage());
        } else {
            return Result.success();
        }
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
        return convertFromMap(response.getSource(), clazz);
    }

    /**
     * 批量获取（mget）
     * @param index
     * @param type
     * @param clazz
     * @param ids
     * @return return the documents of specific id array
     */
    public <T> List<T> getDocs(String index, String type, Class<T> clazz, String... ids) {
        MultiGetResponse multiResp = client.prepareMultiGet().add(index, type, ids).get();
        List<T> result = new ArrayList<>(ids.length);
        for (MultiGetItemResponse itemResp : multiResp) {
            GetResponse response = itemResp.getResponse();
            if (response.isExists()) {
                result.add(convertFromMap(response.getSource(), clazz));
            }
        }
        return result;
    }

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

    // ------------------------------------------------分页搜索---------------------------------------
    /**
     * 深分页查询（针对用户实时查询）
     * @param query
     * @param pageNo
     * @param pageSize
     * @return  page result and map of row record
     */
    public Page<Map<String, Object>> paginationSearch(ESQueryBuilder query, int pageNo, int pageSize) {
        return this.paginationSearch(query, pageNo, pageSize, Map.class).copy();
    }

    /**
     * 深分页查询（针对用户实时查询）
     * @param query      查询条件
     * @param pageNo     页码
     * @param pageSize   页大小
     * @param clazz      返回的行数据类型
     * @return
     */
    public <T> Page<T> paginationSearch(ESQueryBuilder query, int pageNo, 
                                        int pageSize, Class<T> clazz) {
        int from = (pageNo - 1) * pageSize;
        SearchResponse searchResp = query.pagination(client, from, pageSize);
        return buildPage(searchResp, from, pageNo, pageSize, clazz);
    }

    /**
     * 深分页查询（针对用户实时查询）
     * @param search    SearchRequestBuilder
     * @param pageNo    页码
     * @param pageSize  页大小
     * @return
     */
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
     */
    public <T> Page<T> paginationSearch(SearchRequestBuilder search, int pageNo, 
                                        int pageSize, Class<T> clazz) {
        int from = (pageNo - 1) * pageSize;
        search.setSearchType(SearchType.DFS_QUERY_THEN_FETCH); // 深度分布
        search.setFrom(from).setSize(pageSize).setExplain(false);
        return buildPage(search.get(), from, pageNo, pageSize, clazz);
    }

    // -----------------------------------------------排名搜索-----------------------------------------------
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

    // -----------------------------------------------顶部搜索---------------------------------------
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

    // -----------------------------------------------滚动搜索---------------------------------------
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

    // ---------------------------------------------全部搜索-------------------------------------------
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
                    result.add(convertFromMap(hit.getSourceAsMap(), clazz));
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
                result.add(convertFromMap(hit.getSourceAsMap(), clazz));
            }
        });
        return result;
    }

    // ---------------------------------------------聚合汇总-------------------------------------------
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

    /**
     * 销毁:关闭连接，释放资源
     */
    @Override
    public void destroy() {
        logger.info("closing elasticsearch client.....");
        if (client != null) try {
            client.close();
        } catch (Exception e) {
            logger.error("closing elasticsearch client error.", e);
        }
    }

    /**
     * 判断集群是否有可用节点
     * @return true or false
     */
    public boolean isAvailable() {
        return client != null && client.connectedNodes().size() > 0;
    }

    // --------------------------------------scripts-----------------------------------------
    /**
     * 使用脚本更新文档
     * @param index
     * @param type
     * @param id
     * @param source
     */
    public void updateByGroovy(String index, String type, String id, Map<String, Object> source) {
        UpdateRequestBuilder req = client.prepareUpdate().setIndex(index).setType(type).setId(id);
        req.setScript(new Script(ScriptType.INLINE, "groovy", type, source)).get();
    }

    // ------------------------------------private methods------------------------------------
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
            result.add(convertFromMap(hit.getSourceAsMap(), clazz));
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
    private static <T> T convertFromMap(Map<String, Object> data, Class<T> clazz) {
        if (data == null) {
            return null;
        } else if (clazz.isAssignableFrom(data.getClass())) {
            return (T) data;
        } else {
            return ObjectUtils.map2bean(data, clazz);
        }
    }

}
