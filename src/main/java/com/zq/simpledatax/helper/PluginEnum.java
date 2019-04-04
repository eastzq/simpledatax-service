package com.zq.simpledatax.helper;

public enum PluginEnum {
	ORACLE_READER("reader","oraclereader", "com.zq.simpledatax.plugin.reader.oraclereader.OracleReader",""), 
	EXCEL_READER("reader","excelreader", "com.zq.simpledatax.plugin.reader.excelreader.ExcelReader",""),	
	FTP_READER("reader","ftpreader", "com.zq.simpledatax.plugin.reader.ftpreader.FtpReader",""),	
	RDBMS_READER("reader","rdbmsreader", "com.zq.simpledatax.plugin.reader.rdbmsreader.RdbmsReader",""),	
	SQLSERVER_READER("reader","sqlserverreader", "com.zq.simpledatax.plugin.reader.sqlserverreader.SqlServerReader",""),	
	STREAM_READER("reader","streamreader", "com.zq.simpledatax.plugin.reader.streamreader.StreamReader",""),	
	TXT_FILE_READER("reader","txtfilereader", "com.zq.simpledatax.plugin.reader.txtfilereader.TxtFileReader",""),	
	
	ORACLE_WRITER("writer","oraclewriter", "com.zq.simpledatax.plugin.writer.oraclewriter.OracleWriter",""),
	FTP_WRITER("writer","ftpwriter", "com.zq.simpledatax.plugin.writer.ftpwriter.FtpWriter",""),	
	RDBMS_WRITER("writer","rdbmswriter", "com.zq.simpledatax.plugin.reader.rdbmswriter.RdbmsWriter",""),	
	STREAM_WRITER("writer","streamwriter", "com.zq.simpledatax.plugin.writer.streamwriter.StreamWriter",""),	
	TXT_FILE_WRITER("writer","txtfilewriter", "com.zq.simpledatax.plugin.writer.txtfilewriter.TxtFileWriter","");

	private PluginEnum(String type,String name, String className, String desc) {
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
