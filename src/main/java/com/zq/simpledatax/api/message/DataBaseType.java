package com.zq.simpledatax.api.message;

public enum DataBaseType {
    ORACLE("oracle"), SQLSERVER("sqlserver");

    private String dataBaseType;

    public String getDataBaseType() {
        return this.dataBaseType;
    }

    DataBaseType(String dataBaseType) {
        this.dataBaseType = dataBaseType;
    }

    public DataBaseType valueof(String dataBaseType) {
        if (dataBaseType == null || dataBaseType.length() == 0) {
            return null;
        }
        for (DataBaseType em : values()) {
            if (em.getDataBaseType().equals(dataBaseType)) {
                return em;
            }
        }
        return null;
    }
}
