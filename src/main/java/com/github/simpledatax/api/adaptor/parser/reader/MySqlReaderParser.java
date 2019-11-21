package com.github.simpledatax.api.adaptor.parser.reader;

import java.text.MessageFormat;

import org.apache.commons.lang3.StringUtils;

import com.github.simpledatax.api.adaptor.PluginResouce;
import com.github.simpledatax.api.adaptor.exception.DxException;
import com.github.simpledatax.api.adaptor.parser.intf.Parser;
import com.github.simpledatax.api.dto.CommonDbReader;
import com.github.simpledatax.api.dto.DataCollectPlugin;
import com.github.simpledatax.common.util.Configuration;

public class MySqlReaderParser implements Parser {

    @Override
    public Configuration parse(DataCollectPlugin plugin) throws DxException {
        CommonDbReader reader = null;
        if (!(plugin instanceof CommonDbReader)) {
            throw new DxException("插件对象转换异常，当前实例不是DBDataCollectReader类型！");
        } else {
            reader = (CommonDbReader) plugin;
        }
        Configuration configuration = Configuration.newDefault();
        configuration.set("name", PluginResouce.MYSQL_READER.getName());
        configuration.set("parameter.username", reader.getDbUser());
        configuration.set("parameter.password", reader.getDbPassword());
        if (StringUtils.isNotBlank(reader.getSqlScript())) {
            configuration.set("parameter.connection[0].querySql[0]", reader.getSqlScript());
        } else {
            configuration.set("parameter.where", reader.getSqlWhere());
            configuration.set("parameter.column[0]", reader.getColumnStrs());
            configuration.set("parameter.connection[0].table[0]", reader.getTableName());
            configuration.set("parameter.splitPk", reader.getSplitPk());
        }
        String jdbcUrl = MessageFormat.format("jdbc:mysql://{0}:{1}/{2}", reader.getDbIp(), reader.getDbPort(),
                reader.getDbInstanceName());
        configuration.set("parameter.connection[0].jdbcUrl[0]", jdbcUrl);
        configuration.set("parameter.fetchSize", Integer.MIN_VALUE);
        return configuration;
    }
}
