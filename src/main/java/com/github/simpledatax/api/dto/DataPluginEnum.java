package com.github.simpledatax.api.dto;

public enum DataPluginEnum {
    ORACLE("oracle"), SQLSERVER("sqlserver"), MYSQL("mysql"), RMDBS("rmdbs");

    private DataPluginEnum(String type) {
        this.type = type;
    }

    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
