package com.github.simpledatax.api.dto;

public enum DataPluginEnum {
    ORACLE("oracle"), SQLSERVER("sqlserver"), MYSQL_READER("mysqlreader"), MYSQL_WRITER("mysqlwriter"),
    RMDBS_READER("rmdbsreader"), RMDBS_WRITER("rmdbswriter");

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

    public boolean isReader() {
        return this.getType().endsWith("reader");
    }

    public boolean isWriter() {
        return this.getType().endsWith("writer");
    }
}
