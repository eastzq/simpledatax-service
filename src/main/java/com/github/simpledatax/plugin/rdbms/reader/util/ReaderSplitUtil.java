package com.github.simpledatax.plugin.rdbms.reader.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.simpledatax.common.constant.CommonConstant;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.plugin.rdbms.reader.RdbmsReaderConstant;
import com.github.simpledatax.plugin.rdbms.reader.RdbmsReaderKey;
import com.github.simpledatax.plugin.rdbms.util.DataBaseType;

public final class ReaderSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderSplitUtil.class);

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber,DataBaseType dataBaseType) {
        boolean isTableMode = originalSliceConfig.getBool(RdbmsReaderConstant.IS_TABLE_MODE).booleanValue();
        int eachTableShouldSplittedNumber = -1;
        if (isTableMode) {
            // adviceNumber这里是channel数量大小, 即datax并发task数量
            // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
            eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                    adviceNumber, originalSliceConfig.getInt(RdbmsReaderConstant.TABLE_NUMBER_MARK));
        }

        String column = originalSliceConfig.getString(RdbmsReaderKey.COLUMN);
        String where = originalSliceConfig.getString(RdbmsReaderKey.WHERE, null);

        List<Object> conns = originalSliceConfig.getList(RdbmsReaderConstant.CONN_MARK, Object.class);

        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = originalSliceConfig.clone();

            Configuration connConf = Configuration.from(conns.get(i).toString());
            String jdbcUrl = connConf.getString(RdbmsReaderKey.JDBC_URL);
            sliceConfig.set(RdbmsReaderKey.JDBC_URL, jdbcUrl);

            // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
            sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

            sliceConfig.remove(RdbmsReaderConstant.CONN_MARK);

            Configuration tempSlice;

            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(RdbmsReaderKey.TABLE, String.class);

                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                String splitPk = originalSliceConfig.getString(RdbmsReaderKey.SPLIT_PK, null);

                //最终切分份数不一定等于 eachTableShouldSplittedNumber
                boolean needSplitTable = eachTableShouldSplittedNumber > 1
                        && StringUtils.isNotBlank(splitPk);
                if (needSplitTable) {
                    if (tables.size() == 1) {
                        //原来:如果是单表的，主键切分num=num*2+1
                    	//之后改成 num=num*5
                    	//现在强哥不动他了，和adviceNum一致
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 5;
                    }
                    // 尝试对每个表，切分为eachTableShouldSplittedNumber 份
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(RdbmsReaderKey.TABLE, table);

                        List<Configuration> splittedSlices = SingleTableSplitUtil
                                .splitSingleTable(tempSlice, eachTableShouldSplittedNumber,dataBaseType);

                        splittedConfigs.addAll(splittedSlices);
                    }
                } else {
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(RdbmsReaderKey.TABLE, table);
                        String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column,originalSliceConfig,dataBaseType);
                        tempSlice.set(RdbmsReaderKey.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                        splittedConfigs.add(tempSlice);
                    }
                }
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(RdbmsReaderKey.QUERY_SQL, String.class);

                // TODO 是否check 配置为多条语句？？
                for (String querySql : sqls) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(RdbmsReaderKey.QUERY_SQL, querySql);
                    splittedConfigs.add(tempSlice);
                }
            }

        }

        return splittedConfigs;
    }

    public static Configuration doPreCheckSplit(Configuration originalSliceConfig) {
        Configuration queryConfig = originalSliceConfig.clone();
        boolean isTableMode = originalSliceConfig.getBool(RdbmsReaderConstant.IS_TABLE_MODE).booleanValue();

        String splitPK = originalSliceConfig.getString(RdbmsReaderKey.SPLIT_PK);
        String column = originalSliceConfig.getString(RdbmsReaderKey.COLUMN);
        String where = originalSliceConfig.getString(RdbmsReaderKey.WHERE, null);

        List<Object> conns = queryConfig.getList(RdbmsReaderConstant.CONN_MARK, Object.class);

        for (int i = 0, len = conns.size(); i < len; i++){
            Configuration connConf = Configuration.from(conns.get(i).toString());
            List<String> querys = new ArrayList<String>();
            List<String> splitPkQuerys = new ArrayList<String>();
            String connPath = String.format("connection[%d]",i);
            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(RdbmsReaderKey.TABLE, String.class);
                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");
                for (String table : tables) {
                    querys.add(SingleTableSplitUtil.buildQuerySql(column,table,where));
                    if (splitPK != null && !splitPK.isEmpty()){
                        splitPkQuerys.add(SingleTableSplitUtil.genPKSql(splitPK.trim(),table,where));
                    }
                }
                if (!splitPkQuerys.isEmpty()){
                    connConf.set(RdbmsReaderKey.SPLIT_PK_SQL,splitPkQuerys);
                }
                connConf.set(RdbmsReaderKey.QUERY_SQL,querys);
                queryConfig.set(connPath,connConf);
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(RdbmsReaderKey.QUERY_SQL,
                        String.class);
                for (String querySql : sqls) {
                    querys.add(querySql);
                }
                connConf.set(RdbmsReaderKey.QUERY_SQL,querys);
                queryConfig.set(connPath,connConf);
            }
        }
        return queryConfig;
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
                                                              int tableNumber) {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }

}
