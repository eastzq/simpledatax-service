package com.github.simpledatax.plugin.unstructuredstorage.writer;

public class UnstructuredWriterConstant {

	public static final String UTF8 = "UTF8";

	public static final String ZHS16GBK = "ZHS16GBK";

	public static final String MY_FIELD_DELIMITER = ",";

	public static final String MY_FIELD_ENCLOSED = "\"";

	public static final String MY_NEW_LINE = "X0a";

	public static final String DEFAULT_ENCODING = "UTF-8";

	public static final char DEFAULT_FIELD_DELIMITER = ',';

	public static final String DEFAULT_NULL_FORMAT = "\\N";

	public static final String FILE_FORMAT_CSV = "csv";

	public static final String FILE_FORMAT_TEXT = "text";

	// 每个分块10MB，最大10000个分块
	public static final Long MAX_FILE_SIZE = 1024 * 1024 * 10 * 10000L;

	public static final String DEFAULT_SUFFIX = "";
}
