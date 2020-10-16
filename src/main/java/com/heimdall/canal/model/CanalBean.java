package com.heimdall.canal.model;


import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class CanalBean {
    /**
     * 数据库名称
     */
    private String database;
    /**
     * 表名
     */
    private String table;
    /**
     * 动作类型: (新增)INSERT、(更新)UPDATE、(删除)DELETE、(修改表结构)ALTER、(删除表)ERASE
     */
    private String type;

    /**
     * 时间戳
     */
    private long timestamp;

    private BigDecimal aaa = new BigDecimal("54.00");

    /**
     * 是否是DDL语句
     */
    private boolean isDdl;

    /**
     * 当前最新的数据
     */
//    @JsonSerialize()
    private Map<String, Object> data;

    /**
     * 更新之后的数据集合
     */
    private List<Column> afterColumns;

    /**
     * 更新之前的数据集合
     */
    private List<Column> beforeColumns;

    /**
     * sql语句
     */
    private String sql;

    public BigDecimal getAaa() {
        return aaa;
    }

    public void setAaa(BigDecimal aaa) {
        this.aaa = aaa;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isDdl() {
        return isDdl;
    }

    public void setDdl(boolean ddl) {
        isDdl = ddl;
    }

    public List<Column> getAfterColumns() {
        return afterColumns;
    }

    public void setAfterColumns(List<Column> afterColumns) {
        this.afterColumns = afterColumns;
    }

    public List<Column> getBeforeColumns() {
        return beforeColumns;
    }

    public void setBeforeColumns(List<Column> beforeColumns) {
        this.beforeColumns = beforeColumns;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}