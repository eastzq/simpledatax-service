package com.github.simpledatax.plugin;

public enum PluginResouce {
    RDBMS_READER("reader","rdbmsreader", "com.github.simpledatax.plugin.reader.rdbmsreader.RdbmsReader",""),
	MYSQL_READER("reader","mysqlreader", "com.github.simpledatax.plugin.reader.mysql.MySqlReader",""),   
    MYSQL_WRITER("writer", "mysqlwriter", "com.github.simpledatax.plugin.writer.mysqlwriter.MysqlWriter",""),
	RDBMS_WRITER("writer","rdbmswriter", "com.github.simpledatax.plugin.writer.rdbmswriter.RdbmsWriter","");
    
    private PluginResouce(String type, String name, String className,String desc) {
        this.type = type;
        this.name = name;
        this.className = className;
        this.desc = desc;
    }
	private String type;
	private String name;
	private String desc;
	private String className;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
