package com.zq.simpledatax.helper.parser.writer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.zq.simpledatax.api.message.DBDataCollectWriter;
import com.zq.simpledatax.api.message.DataCollectPlugin;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.helper.ConfigHelper;
import com.zq.simpledatax.helper.DbHelper;
import com.zq.simpledatax.helper.PluginEnum;
import com.zq.simpledatax.helper.PropertiesKey;
import com.zq.simpledatax.helper.exception.BusiException;
import com.zq.simpledatax.helper.parser.Validatetor;
import com.zq.simpledatax.helper.parser.intf.Parser;

public class OracleWriterParser implements Parser {

    @Override
    public Configuration parse(DataCollectPlugin plugin) throws BusiException {
        DBDataCollectWriter writer = null;
        if (!(plugin instanceof DBDataCollectWriter)) {
            throw new BusiException("插件对象转换异常，当前实例不是DBDataCollectWriter类型！");
        }
        else {
            writer = (DBDataCollectWriter) plugin;
        }
        validate(writer);
        Configuration configuration = Configuration.newDefault();
        configuration.set("name", PluginEnum.ORACLE_WRITER.getName());
        configuration.set("parameter.username", writer.getDbUser());
        configuration.set("parameter.password", writer.getDbPassword());

        String cols = writer.getColumnStrs();
        List<String> result = Arrays.asList(cols.split(","));
        configuration.set("parameter.column", result);
        configuration.set("parameter.connection[0].table[0]", writer.getTableName());
        String jdbcUrl = DbHelper.getOracleJdbcUrl(writer.getDbIp(), writer.getDbPort(), writer.getDbInstanceName());
        configuration.set("parameter.connection[0].jdbcUrl", jdbcUrl);
        String batchSize = ConfigHelper.getConfigValue(PropertiesKey.JDBC_BATCHSIZE);
        if (StringUtils.isNotBlank(batchSize)) {
            configuration.set("parameter.batchSize", batchSize);
        }
        // 处理固定值字段
        Map<String, Object> fixedColumnMap = writer.getFixedColumns();
        if (fixedColumnMap != null && !fixedColumnMap.isEmpty()) {
            configuration.set("parameter.fixedColumns", fixedColumnMap);
        }

        return configuration;
    }

    /* 读插件参数校验 */
    private void validate(DBDataCollectWriter writer) throws BusiException {
        // 校验必填项
        Validatetor.validateWriter(writer);
    }

}
