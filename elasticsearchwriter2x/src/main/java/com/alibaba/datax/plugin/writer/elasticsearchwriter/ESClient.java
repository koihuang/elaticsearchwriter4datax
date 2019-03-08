package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.exception.DataXException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;

/**
 * Created by huangwei on 19/1/3.
 */
public class ESClient {
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);
    private TransportClient client;

    public void createClient(String hosts, String clustername) throws Exception {
        String[] hostArray = hosts.split(",");
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clustername)
                .build();
        client = TransportClient.builder().settings(settings).build();
        for (String host : hostArray) {
            String ip = host.split(":")[0];
            Integer port = Integer.valueOf(host.split(":")[1]);
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), port));
        }
    }

    public boolean indicesExists(String indexName) {
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }

    public boolean deleteIndex(String indexName) {
        log.info("delete index " + indexName);
        DeleteIndexResponse response = client.admin().indices().prepareDelete(indexName).execute().actionGet();
        if (response.isAcknowledged()) {
            return true;
        }
        return false;
    }

    public boolean createIndex(String indexName, String typeName, String mappings, String settings, boolean dynamic) throws Exception {
        if (!indicesExists(indexName)) {
            log.info("create index" + indexName);
            CreateIndexResponse response = client.admin().indices().prepareCreate(indexName).setSettings(settings).execute().actionGet();
            if (response.isAcknowledged()) {
                log.info(String.format("create [%s] index success", indexName));
            }
        }
        int idx = 0;
        while (idx < 5) {
            if (indicesExists(indexName)) {
                break;
            }
            Thread.sleep(2000);
            idx++;
        }
        if (idx >= 5) {
            return false;
        }
        if (dynamic) {
            log.info("ignore mappings");
            return true;
        }
        log.info("create mappings for " + indexName + "  " + mappings);
        PutMappingRequest mapping = Requests.putMappingRequest(indexName).type(typeName).source(mappings);
        PutMappingResponse response = client.admin().indices().putMapping(mapping).actionGet();
        if (response.isAcknowledged()) {
            log.info(String.format("index [%s] put mappings success", indexName));
        } else {
            log.info(String.format("index [%s] put mappings faild, please check", indexName));
        }
        return true;
    }

    public void closeClient() {
        client.close();
    }

    public boolean alias(String indexName, String alias, boolean needCleanAlias) {
        /*if(indicesExists(alias)) {
            throw new RuntimeException("以别名为名称的索引存在,所以不能用该别名"); // ! 别名也会被当成索引,所以该判断没用
        }*/
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAlias(alias, indexName);
        //获取别名所有相关索引
        GetAliasesResponse response = client.admin().indices().getAliases(new GetAliasesRequest(alias)).actionGet();
        ImmutableOpenMap<String, List<AliasMetaData>> aliases = response.getAliases();
        IndicesAliasesRequestBuilder aliasesRequestBuilder = client.admin().indices().prepareAliases();
        aliasesRequestBuilder.addAlias(indexName, alias);
        if (needCleanAlias) { //如果是清理模式,则删除所有相关的别名设置
            for (String key : aliases.keys().toArray(String.class)) {
                if (indexName.equals(key)) {
                    continue;
                }
                aliasesRequestBuilder.removeAlias(key, alias);
            }
        }
        try {
            IndicesAliasesResponse aliasesResponse = aliasesRequestBuilder.execute().actionGet();
            if(aliasesResponse.isAcknowledged()) {
                return true;
            }
        } catch (InvalidAliasNameException e) {
            throw DataXException.asDataXException(ESWriterErrorCode.ES_ALIAS_SET, e.toString());
        }
        return false;
    }


    public static void main(String[] args) throws Exception {
        ESClient client = new ESClient();
        client.createClient("localhost:9300", "elasticsearch");
        //测试检查索引是否存在方法
        /*boolean indexExists = client.indicesExists("datax-test");
        System.out.println("indexExists = " + indexExists);*/
        //创建索引测试
        // client.createIndex("datax", "doc", "{\"properties\":{\"docName\":{\"type\":\"string\"},\"endNodeId\":{\"type\":\"string\"},\"id\":{\"type\":\"string\"}}}", "{\"index\":{\"number_of_replicas\":1,\"number_of_shards\":5}}", false);

        //别名设置测试
        //client.alias("datax2","datax_alias",true);
    }


    public BulkRequestBuilder prepareBulk() {
        return client.prepareBulk();
    }

    public TransportClient getClient() {
        return client;
    }
}
