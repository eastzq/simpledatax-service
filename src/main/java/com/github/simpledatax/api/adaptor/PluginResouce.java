package com.github.simpledatax.api.adaptor;

public enum PluginResouce {
    RDBMS_READER("reader","rdbmsreader", "com.github.simpledatax.plugin.reader.rdbmsreader.RdbmsReader",""),	
	ORACLE_READER("reader","oraclereader", "com.github.simpledatax.plugin.reader.oraclereader.OracleReader",""), 
	EXCEL_READER("reader","excelreader", "com.github.simpledatax.plugin.reader.excelreader.ExcelReader",""),	
	FTP_READER("reader","ftpreader", "com.github.simpledatax.plugin.reader.ftpreader.FtpReader",""),	
	SQLSERVER_READER("reader","sqlserverreader", "com.github.simpledatax.plugin.reader.sqlserverreader.SqlServerReader",""),	
	STREAM_READER("reader","streamreader", "com.github.simpledatax.plugin.reader.streamreader.StreamReader",""),	
	TXT_FILE_READER("reader","txtfilereader", "com.github.simpledatax.plugin.reader.txtfilereader.TxtFileReader",""),	
	
	RDBMS_WRITER("writer","rdbmswriter", "com.github.simpledatax.plugin.writer.rdbmswriter.RdbmsWriter",""),	
	ORACLE_WRITER("writer","oraclewriter", "com.github.simpledatax.plugin.writer.oraclewriter.OracleWriter",""),
	FTP_WRITER("writer","ftpwriter", "com.github.simpledatax.plugin.writer.ftpwriter.FtpWriter",""),	
	STREAM_WRITER("writer","streamwriter", "com.github.simpledatax.plugin.writer.streamwriter.StreamWriter",""),	
	TXT_FILE_WRITER("writer","txtfilewriter", "com.github.simpledatax.plugin.writer.txtfilewriter.TxtFileWriter","");

    private PluginResouce(String type, String name, String className, String desc) {
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
