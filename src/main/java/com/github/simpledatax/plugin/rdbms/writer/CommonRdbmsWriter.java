package com.github.simpledatax.plugin.rdbms.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.simpledatax.common.element.Column;
import com.github.simpledatax.common.element.Record;
import com.github.simpledatax.common.exception.DataXException;
import com.github.simpledatax.common.plugin.RecordReceiver;
import com.github.simpledatax.common.plugin.TaskPluginCollector;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.plugin.rdbms.util.DBUtil;
import com.github.simpledatax.plugin.rdbms.util.DBUtilErrorCode;
import com.github.simpledatax.plugin.rdbms.util.DataBaseType;
import com.github.simpledatax.plugin.rdbms.util.RdbmsException;
import com.github.simpledatax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.github.simpledatax.plugin.rdbms.writer.util.WriterUtil;

public class CommonRdbmsWriter {

    public static class Job {
        private DataBaseType dataBaseType;

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        public Job(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
        }

        public void init(Configuration originalConfig) {
            OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);

            LOG.debug("After job init(), originalConfig now is:[\n{}\n]", originalConfig.toJSON());
        }

        /* 目前只支持MySQL Writer跟Oracle Writer;检查PreSQL跟PostSQL语法以及insert，delete权限 */
        public void writerPreCheck(Configuration originalConfig, DataBaseType dataBaseType) {
            /* 检查PreSql跟PostSql语句 */
            prePostSqlValid(originalConfig, dataBaseType);
            /* 检查insert 跟delete权限 */
            privilegeValid(originalConfig, dataBaseType);
        }

        public void prePostSqlValid(Configuration originalConfig, DataBaseType dataBaseType) {
            /* 检查PreSql跟PostSql语句 */
            WriterUtil.preCheckPrePareSQL(originalConfig, dataBaseType);
            WriterUtil.preCheckPostSQL(originalConfig, dataBaseType);
        }

        public void privilegeValid(Configuration originalConfig, DataBaseType dataBaseType) {
            /* 检查insert 跟delete权限 */
            String username = originalConfig.getString(RdbmsWriterKey.USERNAME);
            String password = originalConfig.getString(RdbmsWriterKey.PASSWORD);
            List<Object> connections = originalConfig.getList(RdbmsWriterConstant.CONN_MARK, Object.class);

            for (int i = 0, len = connections.size(); i < len; i++) {
                Configuration connConf = Configuration.from(connections.get(i).toString());
                String jdbcUrl = connConf.getString(RdbmsWriterKey.JDBC_URL);
                List<String> expandedTables = connConf.getList(RdbmsWriterKey.TABLE, String.class);
                boolean hasInsertPri = DBUtil.checkInsertPrivilege(dataBaseType, jdbcUrl, username, password,
                        expandedTables);

                if (!hasInsertPri) {
                    throw RdbmsException.asInsertPriException(dataBaseType,
                            originalConfig.getString(RdbmsWriterKey.USERNAME), jdbcUrl);
                }

                if (DBUtil.needCheckDeletePrivilege(originalConfig)) {
                    boolean hasDeletePri = DBUtil.checkDeletePrivilege(dataBaseType, jdbcUrl, username, password,
                            expandedTables);
                    if (!hasDeletePri) {
                        throw RdbmsException.asDeletePriException(dataBaseType,
                                originalConfig.getString(RdbmsWriterKey.USERNAME), jdbcUrl);
                    }
                }
            }
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        public void prepare(Configuration originalConfig) {
            int tableNumber = originalConfig.getInt(RdbmsWriterConstant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(RdbmsWriterKey.USERNAME);
                String password = originalConfig.getString(RdbmsWriterKey.PASSWORD);

                List<Object> conns = originalConfig.getList(RdbmsWriterConstant.CONN_MARK, Object.class);
                Configuration connConf = Configuration.from(conns.get(0).toString());

                // 这里的 jdbcUrl 已经 append 了合适后缀参数
                String jdbcUrl = connConf.getString(RdbmsWriterKey.JDBC_URL);
                originalConfig.set(RdbmsWriterKey.JDBC_URL, jdbcUrl);

                String table = connConf.getList(RdbmsWriterKey.TABLE, String.class).get(0);
                originalConfig.set(RdbmsWriterKey.TABLE, table);

                List<String> preSqls = originalConfig.getList(RdbmsWriterKey.PRE_SQL, String.class);
                List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(preSqls, table);

                originalConfig.remove(RdbmsWriterConstant.CONN_MARK);
                if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
                    // 说明有 preSql 配置，则此处删除掉
                    originalConfig.remove(RdbmsWriterKey.PRE_SQL);

                    Connection conn = DBUtil.getConnection(dataBaseType, jdbcUrl, username, password);
                    LOG.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(renderedPreSqls, ";"),
                            jdbcUrl);

                    WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }

            LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]", originalConfig.toJSON());
        }

        public List<Configuration> split(Configuration originalConfig, int mandatoryNumber) {
            return WriterUtil.doSplit(originalConfig, mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        public void post(Configuration originalConfig) {
            int tableNumber = originalConfig.getInt(RdbmsWriterConstant.TABLE_NUMBER_MARK);
            if (tableNumber == 1) {
                String username = originalConfig.getString(RdbmsWriterKey.USERNAME);
                String password = originalConfig.getString(RdbmsWriterKey.PASSWORD);

                // 已经由 prepare 进行了appendJDBCSuffix处理
                String jdbcUrl = originalConfig.getString(RdbmsWriterKey.JDBC_URL);

                String table = originalConfig.getString(RdbmsWriterKey.TABLE);

                List<String> postSqls = originalConfig.getList(RdbmsWriterKey.POST_SQL, String.class);
                List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(postSqls, table);

                if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
                    // 说明有 postSql 配置，则此处删除掉
                    originalConfig.remove(RdbmsWriterKey.POST_SQL);

                    Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);

                    LOG.info("Begin to execute postSqls:[{}]. context info:{}.",
                            StringUtils.join(renderedPostSqls, ";"), jdbcUrl);
                    WriterUtil.executeSqls(conn, renderedPostSqls, jdbcUrl, dataBaseType);
                    DBUtil.closeDBResources(null, null, conn);
                }
            }
        }

        public void destroy(Configuration originalConfig) {
        }

    }

    public static class Task {
        protected static final Logger LOG = LoggerFactory.getLogger(Task.class);

        protected DataBaseType dataBaseType;
        private static final String VALUE_HOLDER = "?";

        protected String username;
        protected String password;
        protected String jdbcUrl;
        protected String table;
        protected List<String> columns;
        protected List<String> preSqls;
        protected List<String> postSqls;
        protected int batchSize;
        protected int batchByteSize;
        protected int columnNumber = 0;
        protected TaskPluginCollector taskPluginCollector;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        protected static String BASIC_MESSAGE;

        protected static String INSERT_OR_REPLACE_TEMPLATE;

        protected String writeRecordSql;
        protected String writeMode;
        protected boolean emptyAsNull;
        protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;
        protected Map<String, Object> fixedColumns;
        protected int fixedColumnNumber = 0;
        protected List<String> completeColumns;

        public Task(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
        }

        public void init(Configuration writerSliceConfig) {
            this.username = writerSliceConfig.getString(RdbmsWriterKey.USERNAME);
            this.password = writerSliceConfig.getString(RdbmsWriterKey.PASSWORD);
            this.jdbcUrl = writerSliceConfig.getString(RdbmsWriterKey.JDBC_URL);

            // ob10的处理
            if (this.jdbcUrl.startsWith(RdbmsWriterConstant.OB10_SPLIT_STRING)
                    && this.dataBaseType == DataBaseType.MySql) {
                String[] ss = this.jdbcUrl.split(RdbmsWriterConstant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    throw DataXException.asDataXException(DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR,
                            "JDBC OB10格式错误，请联系askdatax");
                }
                LOG.info("this is ob1_0 jdbc url.");
                this.username = ss[1].trim() + ":" + this.username;
                this.jdbcUrl = ss[2];
                LOG.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
            }

            this.table = writerSliceConfig.getString(RdbmsWriterKey.TABLE);

            this.columns = writerSliceConfig.getList(RdbmsWriterKey.COLUMN, String.class);
            this.columnNumber = this.columns.size();
            this.fixedColumns = writerSliceConfig.getMap(RdbmsWriterKey.FIXED_COLUMNS);
            if (this.fixedColumns != null && !this.fixedColumns.isEmpty()) {
                this.fixedColumnNumber = this.fixedColumns.size();

                this.completeColumns = new ArrayList<String>(this.columnNumber + this.fixedColumnNumber);
                this.completeColumns.addAll(this.columns);
                for (Entry<String, Object> entry : this.fixedColumns.entrySet()) {
                    this.completeColumns.add(entry.getKey());
                }
            } else {
                this.completeColumns = this.columns;
            }

            this.preSqls = writerSliceConfig.getList(RdbmsWriterKey.PRE_SQL, String.class);
            this.postSqls = writerSliceConfig.getList(RdbmsWriterKey.POST_SQL, String.class);
            this.batchSize = writerSliceConfig.getInt(RdbmsWriterKey.BATCH_SIZE,
                    RdbmsWriterConstant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(RdbmsWriterKey.BATCH_BYTE_SIZE,
                    RdbmsWriterConstant.DEFAULT_BATCH_BYTE_SIZE);

            writeMode = writerSliceConfig.getString(RdbmsWriterKey.WRITE_MODE, "INSERT");
            emptyAsNull = writerSliceConfig.getBool(RdbmsWriterKey.EMPTY_AS_NULL, true);
            INSERT_OR_REPLACE_TEMPLATE = writerSliceConfig
                    .getString(RdbmsWriterConstant.INSERT_OR_REPLACE_TEMPLATE_MARK);
            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);

            BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]", this.jdbcUrl, this.table);
        }

        public void prepare(Configuration writerSliceConfig) {
            Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);

            DBUtil.dealWithSessionConfig(connection, writerSliceConfig, this.dataBaseType, BASIC_MESSAGE);

            int tableNumber = writerSliceConfig.getInt(RdbmsWriterConstant.TABLE_NUMBER_MARK);
            if (tableNumber != 1) {
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.", StringUtils.join(this.preSqls, ";"),
                        BASIC_MESSAGE);
                WriterUtil.executeSqls(connection, this.preSqls, BASIC_MESSAGE, dataBaseType);
            }

            DBUtil.closeDBResources(null, null, connection);
        }

        public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector,
                Connection connection) {
            this.taskPluginCollector = taskPluginCollector;

            this.resultSetMetaData = DBUtil.getColumnMetaData(connection, this.table,
                    StringUtils.join(this.completeColumns, ","));
            // 写数据库的SQL语句
            calcWriteRecordSql();

            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                                String.format("列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                        record.getColumnNumber(), this.columnNumber));
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        doBatchInsert(connection, writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
                DBUtil.closeDBResources(null, null, connection);
            }
        }

        // TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver, Configuration writerSliceConfig,
                TaskPluginCollector taskPluginCollector) {
            Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);
            DBUtil.dealWithSessionConfig(connection, writerSliceConfig, this.dataBaseType, BASIC_MESSAGE);
            startWriteWithConnection(recordReceiver, taskPluginCollector, connection);
        }

        public void post(Configuration writerSliceConfig) {
            int tableNumber = writerSliceConfig.getInt(RdbmsWriterConstant.TABLE_NUMBER_MARK);

            boolean hasPostSql = (this.postSqls != null && this.postSqls.size() > 0);
            if (tableNumber == 1 || !hasPostSql) {
                return;
            }

            Connection connection = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl, username, password);

            LOG.info("Begin to execute postSqls:[{}]. context info:{}.", StringUtils.join(this.postSqls, ";"),
                    BASIC_MESSAGE);
            WriterUtil.executeSqls(connection, this.postSqls, BASIC_MESSAGE, dataBaseType);
            DBUtil.closeDBResources(null, null, connection);
        }

        public void destroy(Configuration writerSliceConfig) {
        }

        protected void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    preparedStatement = fillPreparedStatement(preparedStatement, record);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        protected void doOneInsert(Connection connection, List<Record> buffer) {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection.prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(preparedStatement, record);
                        preparedStatement.execute();
                    } catch (SQLException e) {
                        LOG.error(e.toString());
                        this.taskPluginCollector.collectDirtyRecord(record, e);
                    } finally {
                        // 最后不要忘了关闭 preparedStatement
                        preparedStatement.clearParameters();
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        // 直接使用了两个类变量：columnNumber,resultSetMetaData
        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
                throws SQLException {
            for (int i = 0; i < this.columnNumber; i++) {
                int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype,
                        record.getColumn(i));
            }

            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement,
                int columnIndex, int columnSqltype, Column column) throws SQLException {
            java.util.Date utilDate;
            switch (columnSqltype) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                preparedStatement.setString(columnIndex + 1, column.asString());
                break;

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                String strValue = column.asString();
                if (emptyAsNull && "".equals(strValue)) {
                    preparedStatement.setString(columnIndex + 1, null);
                } else {
                    preparedStatement.setString(columnIndex + 1, strValue);
                }
                break;

            // tinyint is a little special in some database like mysql {boolean->tinyint(1)}
            case Types.TINYINT:
                Long longValue = column.asLong();
                if (null == longValue) {
                    preparedStatement.setString(columnIndex + 1, null);
                } else {
                    preparedStatement.setString(columnIndex + 1, longValue.toString());
                }
                break;

            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (this.resultSetMetaData.getRight().get(columnIndex).equalsIgnoreCase("year")) {
                    if (column.asBigInteger() == null) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                    }
                } else {
                    java.sql.Timestamp sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format("Date 类型转换错误：[%s]", column));
                    }
                    if (null != utilDate) {
                        sqlTime = new java.sql.Timestamp(utilDate.getTime());
                    }
                    preparedStatement.setTimestamp(columnIndex + 1, sqlTime);
                }
                break;

            case Types.TIME:
                java.sql.Time sqlTime = null;
                try {
                    utilDate = column.asDate();
                } catch (DataXException e) {
                    throw new SQLException(String.format("TIME 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTime = new java.sql.Time(utilDate.getTime());
                }
                preparedStatement.setTime(columnIndex + 1, sqlTime);
                break;

            case Types.TIMESTAMP:
                java.sql.Timestamp sqlTimestamp = null;
                try {
                    utilDate = column.asDate();
                } catch (DataXException e) {
                    throw new SQLException(String.format("TIMESTAMP 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTimestamp = new java.sql.Timestamp(utilDate.getTime());
                }
                preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                preparedStatement.setBytes(columnIndex + 1, column.asBytes());
                break;

            case Types.BOOLEAN:
                preparedStatement.setString(columnIndex + 1, column.asString());
                break;

            // warn: bit(1) -> Types.BIT 可使用setBoolean
            // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
            case Types.BIT:
                if (this.dataBaseType == DataBaseType.MySql) {
                    preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                } else {
                    preparedStatement.setString(columnIndex + 1, column.asString());
                }
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, String.format(
                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                        this.resultSetMetaData.getLeft().get(columnIndex),
                        this.resultSetMetaData.getMiddle().get(columnIndex),
                        this.resultSetMetaData.getRight().get(columnIndex)));
            }
            return preparedStatement;
        }

        /**
         * 支持fixedColumns的处理，即插入固定值
         * 
         * @Description
         * @throws @author wlinc_000
         * @date 2019年2月2日 上午10:56:19
         * @see
         */
        private void calcWriteRecordSql() {
            if (!VALUE_HOLDER.equals(calcValueHolder("")) || this.fixedColumnNumber > 0) {
                List<String> valueHolders = new ArrayList<String>(columnNumber + fixedColumnNumber);
                for (int i = 0; i < columns.size(); i++) {
                    String type = resultSetMetaData.getRight().get(i);
                    valueHolders.add(calcValueHolder(type));
                }
                appendFixedColumnValueHolders(valueHolders);

                boolean forceUseUpdate = false;
                // ob10的处理
                if (dataBaseType != null && dataBaseType == DataBaseType.MySql
                        && OriginalConfPretreatmentUtil.isOB10(jdbcUrl)) {
                    forceUseUpdate = true;
                }

                INSERT_OR_REPLACE_TEMPLATE = WriterUtil.getWriteTemplate(completeColumns, valueHolders, writeMode,
                        dataBaseType, forceUseUpdate);
                writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
            }
        }

        private void appendFixedColumnValueHolders(List<String> valueHolders) {
            if (this.fixedColumnNumber > 0) {
                int i = this.columnNumber;
                for (Entry<String, Object> entry : this.fixedColumns.entrySet()) {
                    int type = resultSetMetaData.getMiddle().get(i);
                    valueHolders.add(calcValueHolder(type, i, entry.getValue()));
                    i++;
                }
            }
        }

        protected String calcValueHolder(int columnSqltype, int columnIndex, Object value) {
            String strValue = null;
            if (value == null) {
                return strValue;
            }
            switch (columnSqltype) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                strValue = "'" + value.toString() + "'";
                break;
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.TINYINT:
                strValue = value.toString();
                if (emptyAsNull && "".equals(strValue)) {
                    strValue = null;
                }
                break;
            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (this.resultSetMetaData.getRight().get(columnIndex).equalsIgnoreCase("year")) {
                    strValue = value.toString();
                    if (emptyAsNull && "".equals(strValue)) {
                        strValue = null;
                    }
                } else {
                    if (value instanceof String) {
                        strValue = "TO_DATE('" + value.toString() + "', 'YYYY-MM-DD HH:MI:SS')";
                    } else if (this.dataBaseType == DataBaseType.SQLServer) {
                        strValue = "CONVERT(datetime, '" + value + "', 20)";
                    } else if (value instanceof Date) {
                        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        strValue = "TO_DATE('" + sf.format((Date) value) + "', 'YYYY-MM-DD HH:MI:SS')";
                    }
                }
                break;
            case Types.TIME:
            case Types.TIMESTAMP:
                if (value instanceof String) {
                    if (this.dataBaseType == DataBaseType.Oracle) {
                        strValue = "TO_DATE('" + value.toString() + "', 'YYYY-MM-DD HH:MI:SS')";
                    } else if (this.dataBaseType == DataBaseType.SQLServer) {
                        strValue = "CONVERT(datetime, '" + value + "', 20)";
                    } else if (this.dataBaseType == DataBaseType.MySql) {
                        strValue = "STR_TO_DATE('" + value.toString() + "', '%Y-%m-%d %H:%I:%S')";
                    }
                } else if (value instanceof Date) {
                    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                    if (this.dataBaseType == DataBaseType.MySql) {
                        strValue = "STR_TO_DATE('" + sf.format((Date) value) + "', '%Y-%m-%d %H:%I:%S')";
                    } else if (this.dataBaseType == DataBaseType.SQLServer) {
                        strValue = "CONVERT(datetime, '" + sf.format((Date) value) + "', 20)";
                    } else {
                        // 默认ORACLE处理
                        strValue = "TO_DATE('" + sf.format((Date) value) + "', 'YYYY-MM-DD HH:MI:SS')";
                    }
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
            case Types.BOOLEAN:
                strValue = "'" + value.toString() + "'";
                break;
            // warn: bit(1) -> Types.BIT 可使用setBoolean
            // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
            case Types.BIT:
                if (this.dataBaseType == DataBaseType.MySql) {
                    strValue = ((Boolean) value == true) ? "true" : "false";
                } else {
                    strValue = "'" + value.toString() + "'";
                }
                break;
            default:
                throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, String.format(
                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                        this.resultSetMetaData.getLeft().get(columnIndex),
                        this.resultSetMetaData.getMiddle().get(columnIndex),
                        this.resultSetMetaData.getRight().get(columnIndex)));
            }
            return strValue;
        }

        protected String calcValueHolder(String columnType) {
            return VALUE_HOLDER;
        }
    }
}
