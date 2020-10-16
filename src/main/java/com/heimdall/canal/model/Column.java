package com.heimdall.canal.model;

/**
 * @author crh
 * @since 2020/10/14
 */
public class Column {

    private String name;

    private Object value;

    private Boolean updated;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Boolean getUpdated() {
        return updated;
    }

    public void setUpdated(Boolean updated) {
        this.updated = updated;
    }
}
