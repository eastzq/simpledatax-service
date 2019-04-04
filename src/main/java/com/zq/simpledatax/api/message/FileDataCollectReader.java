package com.zq.simpledatax.api.message;

public class FileDataCollectReader implements DataCollectReader {

    /**
     * 序列化id
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * 默认跳过行数：0
     */
    public static final int DEFAULT_SKIP_ROWS = 0;
    
    /**
     * 字段分隔符：TAB
     */
    public static final String TAB_FIELD_TERMINATED = "X09";
    
    /**
     * 默认字符封闭符：双引号
     */
    public static final String DEFAULT_FIELD_ENCLOSED = "\"";

    /**
     * 文件编码：GBK
     */
    public static final String GBK = "ZHS16GBK";
    
    /**
     * 文件编码：UTF-8
     */
    public static final String UTF8 = "UTF8";

    public FileDataCollectReader() {
    }
    
    /** 文件名 */
    private String fileName;
    
    /** 文件全路径 */
    private String filePath;

    /**
     * 跳过多少行bcp
     */
    private int skipRows;
    
    /** 文件类型 默认是dbf*/
    private FileType fileType = FileType.DBF;

    /** 文件编码 */
    private String fileEncoding;

    /** 字段字符串，以逗号分隔 */
    private String columnStrs;

    /** 字段信息数组 */
    private Column[] Columns;

    /** 字段分隔符 */
    private String fieldTerminated;

    /** 字段封闭符 */
    private String fieldEnclosed;

    /** 行结束符 */
    private String lineEndSymbol;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public int getSkipRows() {
        return skipRows;
    }

    public void setSkipRows(int skipRows) {
        this.skipRows = skipRows;
    }

    public String getFileEncoding() {
        return fileEncoding;
    }

    public void setFileEncoding(String fileEncoding) {
        this.fileEncoding = fileEncoding;
    }

    public String getColumnStrs() {
        return columnStrs;
    }

    public void setColumnStrs(String columnStrs) {
        this.columnStrs = columnStrs;
    }

    public Column[] getColumns() {
        return Columns;
    }

    public void setColumns(Column[] columns) {
        Columns = columns;
    }

    public String getFieldTerminated() {
        return fieldTerminated;
    }

    public void setFieldTerminated(String fieldTerminated) {
        this.fieldTerminated = fieldTerminated;
    }

    public String getFieldEnclosed() {
        return fieldEnclosed;
    }

    public void setFieldEnclosed(String fieldEnclosed) {
        this.fieldEnclosed = fieldEnclosed;
    }

    public String getLineEndSymbol() {
        return lineEndSymbol;
    }

    public void setLineEndSymbol(String lineEndSymbol) {
        this.lineEndSymbol = lineEndSymbol;
    }

	@Override
	public PluginType getPluginType() {
		return PluginType.READER;
	}

	@Override
	public PluginType getReaderType() {
		return PluginType.FILE;
	}

	@Override
	public String getPluginKey() {
		return getPluginType().getType()+"."+getReaderType().getType()+"."+getFileType().getFileType();
	}
}
