package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by huangwei on 19/1/3.
 */
public class ESWriter extends Writer {
    private final static String WRITE_COLUMNS = "write_columns";

    public static class Job extends Writer.Job {

        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            ESClient esClient = new ESClient();
            try {
                esClient.createClient(Key.getEndpoint(conf), Key.getClusterName(conf));
            } catch (Exception e) {
                throw DataXException.asDataXException(ESWriterErrorCode.ES_CLIENT_CREATE, e.toString());
            }
            String indexName = Key.getIndexName(conf);
            String typeName = Key.getTypeName(conf);
            boolean dynamic = Key.getDynamic(conf);
            String mappings = genMappings(typeName);
            String settings = JSONObject.toJSONString(
                    Key.getSettings(conf)
            );
            log.info(String.format("index:[%s], type:[%s], mappings:[%s]", indexName, typeName, mappings));
            try {
                boolean isIndicesExists = esClient.indicesExists(indexName);
                if (Key.isCleanup(this.conf) && isIndicesExists) {
                    esClient.deleteIndex(indexName);
                }
                // 索引已经存在的情况下不创建索引
                if(!isIndicesExists) {
                    if (!esClient.createIndex(indexName, typeName, mappings, settings, dynamic)) {
                        throw new IOException("create index or mapping failed");
                    }
                }

            } catch (Exception ex) {
                throw DataXException.asDataXException(ESWriterErrorCode.ES_MAPPINGS, ex.toString());
            }
            esClient.closeClient();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void post() {
            ESClient esClient = new ESClient();
            try {
                esClient.createClient(Key.getEndpoint(conf), Key.getClusterName(conf));
            } catch (Exception e) {
                throw DataXException.asDataXException(ESWriterErrorCode.ES_CLIENT_CREATE, e.toString());
            }
            String alias = Key.getAlias(conf);
            if (!"".equals(alias)) {
                log.info(String.format("alias [%s] to [%s]", alias, Key.getIndexName(conf)));
                esClient.alias(Key.getIndexName(conf), alias, Key.isNeedCleanAlias(conf));
            }

        }

        @Override
        public void destroy() {

        }

        private String genMappings(String typeName) {
            String mappings = null;
            Map<String, Object> propMap = new HashMap<String, Object>();
            List<ESColumn> columnList = new ArrayList<ESColumn>();

            List column = conf.getList("column");
            if (column != null) {
                for (Object col : column) {
                    JSONObject jo = JSONObject.parseObject(col.toString());
                    String colName = jo.getString("name");
                    String colTypeStr = jo.getString("type");
                    if (colTypeStr == null) {
                        throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " column must have type");
                    }
                    ESFieldType colType = ESFieldType.getESFieldType(colTypeStr);
                    if (colType == null) {
                        throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " unsupported type");
                    }

                    ESColumn columnItem = new ESColumn();

                    if (colName.equals(Key.PRIMARY_KEY_COLUMN_NAME)) {
                        // 兼容已有版本
                        colType = ESFieldType.ID;
                        colTypeStr = "id";
                    }

                    columnItem.setName(colName);
                    columnItem.setType(colTypeStr);

                    if (colType == ESFieldType.ID) {
                        columnList.add(columnItem);
                        // 如果是id,则properties为空
                        continue;
                    }

                    Boolean array = jo.getBoolean("array");
                    if (array != null) {
                        columnItem.setArray(array);
                    }
                    Map<String, Object> field = new HashMap<String, Object>();
                    field.put("type", colTypeStr);
                    //https://www.elastic.co/guide/en/elasticsearch/reference/5.2/breaking_50_mapping_changes.html#_literal_index_literal_property
                    // https://www.elastic.co/guide/en/elasticsearch/guide/2.x/_deep_dive_on_doc_values.html#_disabling_doc_values
                    field.put("doc_values", jo.getBoolean("doc_values"));
                    field.put("ignore_above", jo.getInteger("ignore_above"));
                    field.put("index", jo.getBoolean("index"));

                    switch (colType) {
                        case STRING:
                            // 兼容string类型,ES5之前版本
                            break;
                        case KEYWORD:
                            // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-search-speed.html#_warm_up_global_ordinals
                            field.put("eager_global_ordinals", jo.getBoolean("eager_global_ordinals"));
                        case TEXT:
                            field.put("analyzer", jo.getString("analyzer"));
                            // 优化disk使用,也同步会提高index性能
                            // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-disk-usage.html
                            field.put("norms", jo.getBoolean("norms"));
                            field.put("index_options", jo.getBoolean("index_options"));
                            break;
                        case DATE:
                            columnItem.setTimeZone(jo.getString("timezone"));
                            columnItem.setFormat(jo.getString("format"));
                            // 后面时间会处理为带时区的标准时间,所以不需要给ES指定格式
                            /*
                            if (jo.getString("format") != null) {
                                field.put("format", jo.getString("format"));
                            } else {
                                //field.put("format", "strict_date_optional_time||epoch_millis||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd");
                            }
                            */
                            break;
                        case GEO_SHAPE:
                            field.put("tree", jo.getString("tree"));
                            field.put("precision", jo.getString("precision"));
                        default:
                            break;
                    }
                    propMap.put(colName, field);
                    columnList.add(columnItem);
                }
            }
            conf.set(WRITE_COLUMNS, JSON.toJSONString(columnList));

            log.info(JSON.toJSONString(columnList));

            Map<String, Object> rootMappings = new HashMap<String, Object>();
            Map<String, Object> typeMappings = new HashMap<String, Object>();
            typeMappings.put("properties", propMap);
            rootMappings.put(typeName, typeMappings);

            mappings = JSON.toJSONString(rootMappings);

            if (mappings == null || "".equals(mappings)) {
                throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, "must have mappings");
            }

            return mappings;
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Configuration conf;

        ESClient esClient = null;
        private List<ESFieldType> typeList;
        private List<ESColumn> columnList;

        private int trySize;
        private int batchSize;
        private String index;
        private String type;
        private String splitter;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            index = Key.getIndexName(conf);
            type = Key.getTypeName(conf);

            trySize = Key.getTrySize(conf);
            batchSize = Key.getBatchSize(conf);
            splitter = Key.getSplitter(conf);
            columnList = JSON.parseObject(this.conf.getString(WRITE_COLUMNS), new TypeReference<List<ESColumn>>(){} );
            typeList = new ArrayList<ESFieldType>();
            for (ESColumn col:columnList) {
                typeList.add(ESFieldType.getESFieldType(col.getType()));
            }
            esClient = new ESClient();
        }

        @Override
        public void prepare() {
            try {
                esClient.createClient(Key.getEndpoint(conf), Key.getClusterName(conf));
            } catch (Exception e) {
                throw DataXException.asDataXException(ESWriterErrorCode.ES_CLIENT_CREATE, e.toString());
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            long total = 0;
            while((record = recordReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if(writerBuffer.size() >= this.batchSize) {
                    total += doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
            }
            if(!writerBuffer.isEmpty()) {
                total += doBatchInsert(writerBuffer);
                writerBuffer.clear();
            }

            String msg = String.format("task end,write size : %d",total);
            getTaskPluginCollector().collectMessage("writesize",String.valueOf(total));
            log.info(msg);
            esClient.getClient().close();
        }

        private long doBatchInsert(List<Record> writerBuffer) {
            Map<String,Object> data = null;
            BulkRequestBuilder bulkRequest = esClient.getClient().prepareBulk();
            for(Record record : writerBuffer) {
                data = new HashMap<String, Object>();
                String id = null;
                for (int i = 0; i < record.getColumnNumber(); i++) {
                    Column column = record.getColumn(i);
                    String columnName = columnList.get(i).getName();
                    ESFieldType columnType = typeList.get(i);
                    if(columnList.get(i).isArray() != null && columnList.get(i).isArray()) {
                        String[] dataList = column.asString().split(splitter);
                        if(!columnType.equals(ESFieldType.DATE)) {
                            data.put(columnName,dataList);
                        } else {
                            for (int pos = 0; pos < dataList.length; pos++) {
                                dataList[pos] = getDateStr(columnList.get(i),column);
                            }
                            data.put(columnName,dataList);
                        }
                    } else {
                        switch (columnType) {
                            case ID:
                                if(id != null) {
                                    id += record.getColumn(i).asString();
                                } else {
                                    id = record.getColumn(i).asString();
                                }
                                break;
                            case DATE:
                                try {
                                    String dateStr = getDateStr(columnList.get(i), column);
                                    data.put(columnName, dateStr);
                                }catch (Exception e) {
                                    getTaskPluginCollector().collectDirtyRecord(record, String.format("时间类型解析失败 [%s:%s] exception: %s", columnName, column.toString(), e.toString()));
                                }
                                break;
                            case KEYWORD:
                            case STRING:
                            case TEXT:
                            case IP:
                            case GEO_POINT:
                                data.put(columnName,column.asString());
                                break;
                            case BOOLEAN:
                                data.put(columnName,column.asBoolean());
                                break;
                            case BYTE:
                            case BINARY:
                                data.put(columnName,column.asBytes());
                                break;
                            case LONG:
                                data.put(columnName,column.asLong());
                                break;
                            case INTEGER:
                                data.put(columnName,column.asBigInteger());
                                break;
                            case SHORT:
                                data.put(columnName,column.asBigInteger());
                                break;
                            case FLOAT:
                            case DOUBLE:
                                data.put(columnName,column.asDouble());
                                break;
                            case NESTED:
                            case OBJECT:
                            case GEO_SHAPE:
                                data.put(columnName,JSON.parse(column.asString()));
                                break;
                            default:
                                getTaskPluginCollector().collectDirtyRecord(record,"类型错误:不支持的类型:" + columnType + " " + columnName);
                        }
                    }
                }
                if(id == null) {
                    bulkRequest.add(esClient.getClient().prepareIndex(index,type).setSource(data));
                } else {
                    bulkRequest.add(esClient.getClient().prepareIndex(index,type,id).setSource(data));
                }
            }
            BulkResponse bulkResponse = bulkRequest.get();
            if(bulkResponse.hasFailures()) {
                log.error(bulkResponse.buildFailureMessage());
            };
            return writerBuffer.size();
        }

        private String getDateStr(ESColumn esColumn, Column column) {
            DateTime date = null;
            DateTimeZone dtz = DateTimeZone.getDefault();
            if(esColumn.getTimezone() != null) {
                dtz = DateTimeZone.forID(esColumn.getTimezone());
            }
            if(column.getType() != Column.Type.DATE && esColumn.getFormat() != null) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(esColumn.getFormat());
                date = formatter.withZone(dtz).parseDateTime(column.asString());
                return date.toString();
            } else if (column.getType() == Column.Type.DATE) {
                date = new DateTime(column.asLong(),dtz) ;
                return date.toString();
            } else {
                return column.asString();
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {

        }
    }
}
