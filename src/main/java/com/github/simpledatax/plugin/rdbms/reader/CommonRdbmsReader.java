package com.github.simpledatax.plugin.rdbms.reader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.simpledatax.common.element.BoolColumn;
import com.github.simpledatax.common.element.BytesColumn;
import com.github.simpledatax.common.element.DateColumn;
import com.github.simpledatax.common.element.DoubleColumn;
import com.github.simpledatax.common.element.LongColumn;
import com.github.simpledatax.common.element.Record;
import com.github.simpledatax.common.element.StringColumn;
import com.github.simpledatax.common.exception.DataXException;
import com.github.simpledatax.common.plugin.RecordSender;
import com.github.simpledatax.common.plugin.TaskPluginCollector;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.github.simpledatax.plugin.rdbms.reader.util.PreCheckTask;
import com.github.simpledatax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.github.simpledatax.plugin.rdbms.util.DBUtil;
import com.github.simpledatax.plugin.rdbms.util.DBUtilErrorCode;
import com.github.simpledatax.plugin.rdbms.util.DataBaseType;
import com.github.simpledatax.plugin.rdbms.util.RdbmsException;
import com.google.common.collect.Lists;

public class CommonRdbmsReader {

    public static class Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private DataBaseType dataBaseType;

        public Job(DataBaseType dataBaseType) {
            this.dataBaseType = dataBaseType;
        }

        public void init(Configuration originalConfig) {

            OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);

            LOG.debug("After job init(), job config now is:[\n{}\n]", originalConfig.toJSON());
        }

        public void preCheck(Configuration originalConfig, DataBaseType dataBaseType) {
            /* 检查每个表是否有读权限，以及querySql跟splik Key是否正确 */
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConfig);
            String splitPK = queryConf.getString(RdbmsReaderKey.SPLIT_PK);
            List<Object> connList = queryConf.getList(RdbmsReaderConstant.CONN_MARK, Object.class);
            String username = queryConf.getString(RdbmsReaderKey.USERNAME);
            String password = queryConf.getString(RdbmsReaderKey.PASSWORD);
            ExecutorService exec;
            if (connList.size() < 10) {
                exec = Executors.newFixedThreadPool(connList.size());
            } else {
                exec = Executors.newFixedThreadPool(10);
            }
            Collection<PreCheckTask> taskList = new ArrayList<PreCheckTask>();
            for (int i = 0, len = connList.size(); i < len; i++) {
                Configuration connConf = Configuration.from(connList.get(i).toString());
                PreCheckTask t = new PreCheckTask(username, password, connConf, dataBaseType, splitPK);
                taskList.add(t);
            }
            List<Future<Boolean>> results = Lists.newArrayList();
            try {
                results = exec.invokeAll(taskList);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            for (Future<Boolean> result : results) {
                try {
                    result.get();
                } catch (ExecutionException e) {
                    DataXException de = (DataXException) e.getCause();
                    throw de;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            exec.shutdownNow();
        }

        public List<Configuration> split(Configuration originalConfig, int adviceNumber) {
            return ReaderSplitUtil.doSplit(originalConfig, adviceNumber, this.dataBaseType);
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }

    }

    public static class Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        private DataBaseType dataBaseType;
        private int taskGroupId = -1;
        private int taskId = -1;

        private String username;
        private String password;
        private String jdbcUrl;
        private String mandatoryEncoding;

        // 作为日志显示信息时，需要附带的通用信息。比如信息所对应的数据库连接等信息，针对哪个表做的操作
        private String basicMsg;

        public Task(DataBaseType dataBaseType) {
            this(dataBaseType, -1, -1);
        }

        public Task(DataBaseType dataBaseType, int taskGropuId, int taskId) {
            this.dataBaseType = dataBaseType;
            this.taskGroupId = taskGropuId;
            this.taskId = taskId;
        }

        public void init(Configuration readerSliceConfig) {

            /* for database connection */

            this.username = readerSliceConfig.getString(RdbmsReaderKey.USERNAME);
            this.password = readerSliceConfig.getString(RdbmsReaderKey.PASSWORD);
            this.jdbcUrl = readerSliceConfig.getString(RdbmsReaderKey.JDBC_URL);

            // ob10的处理
            if (this.jdbcUrl.startsWith(com.github.simpledatax.plugin.rdbms.writer.RdbmsWriterConstant.OB10_SPLIT_STRING)
                    && this.dataBaseType == DataBaseType.MySql) {
                String[] ss = this.jdbcUrl
                        .split(com.github.simpledatax.plugin.rdbms.writer.RdbmsWriterConstant.OB10_SPLIT_STRING_PATTERN);
                if (ss.length != 3) {
                    throw DataXException.asDataXException(DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR,
                            "JDBC OB10格式错误，请联系askdatax");
                }
                LOG.info("this is ob1_0 jdbc url.");
                this.username = ss[1].trim() + ":" + this.username;
                this.jdbcUrl = ss[2];
                LOG.info("this is ob1_0 jdbc url. user=" + this.username + " :url=" + this.jdbcUrl);
            }

            this.mandatoryEncoding = readerSliceConfig.getString(RdbmsReaderKey.MANDATORY_ENCODING, "");

            basicMsg = String.format("jdbcUrl:[%s]", this.jdbcUrl);

        }

        public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
                TaskPluginCollector taskPluginCollector, int fetchSize) {
            String querySql = readerSliceConfig.getString(RdbmsReaderKey.QUERY_SQL);
            String table = readerSliceConfig.getString(RdbmsReaderKey.TABLE);

            LOG.info("Begin to read record by Sql: [{}\n] {}.", querySql, basicMsg);
            Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl, username, password);

            // session config .etc related
            DBUtil.dealWithSessionConfig(conn, readerSliceConfig, this.dataBaseType, basicMsg);

            int columnNumber = 0;
            ResultSet rs = null;
            try {
                //如果mysql不设置这个属性会出现oom错误。
                if(dataBaseType==DataBaseType.MySql) {
                    fetchSize=Integer.MIN_VALUE;
                }
                rs = DBUtil.query(conn, querySql, fetchSize);
                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();
                
                long rsNextUsedTime = 0;
                long lastTime = System.nanoTime();
                while (rs.next()) {
                    rsNextUsedTime += (System.nanoTime() - lastTime);
                    this.transportOneRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding,
                            taskPluginCollector);
                    lastTime = System.nanoTime();
                }
                // 目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
                LOG.info("Finished read record by Sql: [{}\n] {}.", querySql, basicMsg);

            } catch (Exception e) {
                recordSender.terminate();
                throw RdbmsException.asQueryException(this.dataBaseType, e, querySql, table, username);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }

        public void post(Configuration originalConfig) {
            // do nothing
        }

        public void destroy(Configuration originalConfig) {
            // do nothing
        }

        protected Record transportOneRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData,
                int columnNumber, String mandatoryEncoding, TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding,
                    taskPluginCollector);
            recordSender.sendToWriter(record);
            return record;
        }

        protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData,
                int columnNumber, String mandatoryEncoding, TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        String rawData;
                        if (StringUtils.isBlank(mandatoryEncoding)) {
                            rawData = rs.getString(i);
                        } else {
                            rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY : rs.getBytes(i)),
                                    mandatoryEncoding);
                        }
                        record.addColumn(new StringColumn(rawData));
                        break;

                    case Types.CLOB:
                    case Types.NCLOB:
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;

                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        record.addColumn(new LongColumn(rs.getString(i)));
                        break;

                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.TIME:
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;

                    // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                    case Types.DATE:
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                            record.addColumn(new LongColumn(rs.getInt(i)));
                        } else {
                            record.addColumn(new DateColumn(rs.getDate(i)));
                        }
                        break;

                    case Types.TIMESTAMP:
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;

                    // warn: bit(1) -> Types.BIT 可使用BoolColumn
                    // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;

                    case Types.NULL:
                        String stringData = null;
                        if (rs.getObject(i) != null) {
                            stringData = rs.getObject(i).toString();
                        }
                        record.addColumn(new StringColumn(stringData));
                        break;

                    default:
                        throw DataXException.asDataXException(DBUtilErrorCode.UNSUPPORTED_TYPE, String.format(
                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                metaData.getColumnName(i), metaData.getColumnType(i), metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString() + " occur exception:", e);
                }
                // TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }
    }

}
