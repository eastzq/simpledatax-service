package com.zq.simpledatax.api.message;

import java.util.Map;

public class DBDataCollectWriter implements DataCollectWriter {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /** 数据库类型 默认oracle**/
    private DataBaseType dbType = DataBaseType.ORACLE;
    
    /** 数据库实例IP */
    private String dbIp;

    /** 数据库实例端口 */
    private String dbPort;

    /** 数据库实例名 */
    private String dbInstanceName;

    /** 登录用户 */
    private String dbUser;

    /** 登录密码 */
    private String dbPassword;

    /** 接口表名 */
    private String tableName;

    /** 字段串 */
    private String columnStrs;

    /**
     * dblink模式下有效。
     */
    private String dblinkName;

    /** 固定字段，在导入的时候key作为字段名，value为固定字段值，key-value默认类型是String*/
    private Map<String, Object> fixedColumns;
    /**
     * 读写方式，使用jdbc还是文件导入到数据库中！默认jdbc
     * @return
     */
    private ReadWriteMode readWriteMode = ReadWriteMode.JDBC;
    
    public String getDbIp() {
        return dbIp;
    }

    public void setDbIp(String dbIp) {
        this.dbIp = dbIp;
    }

    public String getDbPort() {
        return dbPort;
    }

    public void setDbPort(String dbPort) {
        this.dbPort = dbPort;
    }

    public String getDbInstanceName() {
        return dbInstanceName;
    }

    public void setDbInstanceName(String dbInstanceName) {
        this.dbInstanceName = dbInstanceName;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnStrs() {
        return columnStrs;
    }

    public void setColumnStrs(String columnStrs) {
        this.columnStrs = columnStrs;
    }


	public DataBaseType getDbType() {
		return dbType;
	}

	public void setDbType(DataBaseType dbType) {
		this.dbType = dbType;
	}

	public ReadWriteMode getReadWriteMode() {
		return readWriteMode;
	}

	public void setReadWriteMode(ReadWriteMode readWriteMode) {
		this.readWriteMode = readWriteMode;
	}

	public String getDblinkName() {
		return dblinkName;
	}

	public void setDblinkName(String dblinkName) {
		this.dblinkName = dblinkName;
	}

	@Override
	public PluginType getPluginType() {
		return PluginType.WRITER;
	}

	@Override
	public PluginType getWriterType() {
		return PluginType.DB;
	}

	@Override
	public String getPluginKey() {
		return getPluginType().getType()+"."+getWriterType().getType()+"."+getDbType().getDataBaseType();
	}

    public Map<String, Object> getFixedColumns() {
        return fixedColumns;
    }

    public void setFixedColumns(Map<String, Object> fixedColumns) {
        this.fixedColumns = fixedColumns;
    }
}
