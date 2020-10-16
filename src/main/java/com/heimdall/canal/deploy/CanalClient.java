package com.heimdall.canal.deploy;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.heimdall.canal.model.CanalBean;
import com.heimdall.canal.model.Column;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author crh
 * @since 2020/10/13
 */
@Component
public class CanalClient implements InitializingBean {

    private final static int BATCH_SIZE = 1000;

    @Override
    public void afterPropertiesSet() throws Exception {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1", 11111), "example", "", "");
        try {
            //打开连接
            connector.connect();
            //订阅数据库表,全部表
            connector.subscribe(".*\\..*");
            //回滚到未进行ack的地方，下次fetch的时候，可以从最后一个没有ack的地方开始拿
            connector.rollback();
            while (true) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(BATCH_SIZE);
                //获取批量ID
                long batchId = message.getId();
                //获取批量的数量
                int size = message.getEntries().size();
                //如果没有数据
                if (batchId == -1 || size == 0) {
                    try {
                        //线程休眠2秒
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    //如果有数据,处理数据
                    printEntry(message.getEntries());
                }
                //进行 batch id 的确认。确认之后，小于等于此 batchId 的 Message 都会被确认。
                connector.ack(batchId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }
    }

    /**
     * 打印canal server解析binlog获得的实体类信息
     */
    private static void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                //开启/关闭事务的实体类型，跳过
                continue;
            }
            //RowChange对象，包含了一行数据变化的所有特征
            //比如isDdl 是否是ddl变更操作 sql 具体的ddl sql beforeColumns afterColumns 变更前后的数据字段等等
            CanalEntry.RowChange rowChage;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }
            //获取操作类型：insert/update/delete类型
            CanalEntry.EventType eventType = rowChage.getEventType();
            //打印Header信息
            System.out.println(String.format("================》; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            //判断是否是DDL语句

            if (rowChage.getIsDdl()) {
                System.out.println("================》;isDdl: true, sql:" + rowChage.getSql());
            }


            //获取RowChange对象里的每一行数据，打印出来
            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                CanalBean canalBean = new CanalBean();
                canalBean.setDatabase(entry.getHeader().getSchemaName());
                canalBean.setTable(entry.getHeader().getTableName());
                canalBean.setType(rowChage.getEventType().name());
                canalBean.setTimestamp(System.currentTimeMillis());
                canalBean.setDdl(rowChage.getIsDdl());
                canalBean.setSql(rowChage.getSql());
                canalBean.setAfterColumns(assembleColumns(rowData.getAfterColumnsList()));
                canalBean.setBeforeColumns(assembleColumns(rowData.getBeforeColumnsList()));
                canalBean.setData(assembleData(rowData.getAfterColumnsList()));
                printColumn(canalBean);
//                if (eventType == EventType.DELETE) {
//                    printColumn(rowData.getBeforeColumnsList());
//                } else if (eventType == EventType.INSERT) {
//                    printColumn(rowData.getAfterColumnsList());
//                } else {
//                    System.out.println("-------&gt; before");
//                    printColumn(rowData.getBeforeColumnsList());
//                    System.out.println("-------&gt; after");
//                    printColumn(rowData.getAfterColumnsList());
//                }
            }
        }
    }

    private static void printColumn(CanalBean canalBean) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            System.out.println(objectMapper.writeValueAsString(canalBean));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Object> assembleData(List<CanalEntry.Column> columns) {
        Map<String, Object> map = new LinkedHashMap<>(columns.size());
        for (CanalEntry.Column column : columns) {
            map.put(column.getName(), convertValue(column));
        }
        return map;
    }

    private static Object convertValue(CanalEntry.Column column) {
        String mysqlType = column.getMysqlType();
        int index = column.getMysqlType().indexOf('(');
        if (index > -1) {
            mysqlType = column.getMysqlType().substring(0, index);
        }
        switch (mysqlType) {

            default:
                throw new RuntimeException(String.format("unSupport mysql type with %s", column.getMysqlType()));
        }
        typeClass.put("tinyint", Integer::parseInt);
        typeClass.put("smallint", Integer::parseInt);
        typeClass.put("int", Integer::parseInt);
        typeClass.put("bigint", Long::parseLong);
        typeClass.put("char", v -> v);
        typeClass.put("varchar", v -> v);
        typeClass.put("tinytext", v -> v);
        typeClass.put("text", v -> v);
        typeClass.put("mediumtext", v -> v);
        typeClass.put("longtext", v -> v);
        typeClass.put("decimal", BigDecimal::new);
        typeClass.put("date", v -> v);
        typeClass.put("time", v -> v);
        typeClass.put("datetime", v -> v);
        typeClass.put("timestamp", Long::parseLong);
        typeClass.put("json", v -> v);
        typeClass.put("float", BigDecimal::new);
        typeClass.put("double", BigDecimal::new);
        if (value instanceof BigDecimal) {
            String[] split = column.getMysqlType().split(",");
            if (split.length == 2) {
                String scale = split[1].substring(0, split.length - 1);
                value = ((BigDecimal) value).setScale(Integer.parseInt(scale));
            }
        }
    }

    private static List<Column> assembleColumns(List<CanalEntry.Column> columns) {
        return columns.stream()
                .map(e -> {
                    Column column = new Column();
                    column.setName(e.getName());
                    column.setValue(e.getValue());
                    column.setUpdated(e.getUpdated());
                    return column;
                })
                .collect(Collectors.toList());
    }


}