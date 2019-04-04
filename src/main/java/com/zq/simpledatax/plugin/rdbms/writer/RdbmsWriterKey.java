package com.zq.simpledatax.plugin.rdbms.writer;


public final class RdbmsWriterKey {
	public static final String JOB_TEMP_PATH = "jobTempPath";
	
    public final static String JDBC_URL = "jdbcUrl";

    public final static String USERNAME = "username";

    public final static String PASSWORD = "password";

    public final static String TABLE = "table";

    public final static String COLUMN = "column";

    //可选值为：insert,replace，默认为 insert （mysql 支持，oracle 没用 replace 机制，只能 insert,oracle 可以不暴露这个参数）
    public final static String WRITE_MODE = "writeMode";

    public final static String PRE_SQL = "preSql";

    public final static String POST_SQL = "postSql";

    public final static String TDDL_APP_NAME = "appName";

    //默认值：256
    public final static String BATCH_SIZE = "batchSize";

    //默认值：32m
    public final static String BATCH_BYTE_SIZE = "batchByteSize";

    public final static String EMPTY_AS_NULL = "emptyAsNull";

    public final static String DB_NAME_PATTERN = "dbNamePattern";

    public final static String DB_RULE = "dbRule";

    public final static String TABLE_NAME_PATTERN = "tableNamePattern";

    public final static String TABLE_RULE = "tableRule";

    public final static String DRYRUN = "dryRun";
    /**
     * 固定值字段
     * @author wlin_000 2019-02-02 增加
     */
    public final static String FIXED_COLUMNS = "fixedColumns";
}