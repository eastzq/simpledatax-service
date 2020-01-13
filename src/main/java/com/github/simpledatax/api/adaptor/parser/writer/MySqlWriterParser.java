package com.github.simpledatax.api.adaptor.parser.writer;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.github.simpledatax.api.adaptor.ConfigHelper;
import com.github.simpledatax.api.adaptor.exception.DxException;
import com.github.simpledatax.api.adaptor.parser.intf.Parser;
import com.github.simpledatax.api.adaptor.util.PropertiesKey;
import com.github.simpledatax.api.dto.MySqlDbWriter;
import com.github.simpledatax.api.dto.DataCollectPlugin;
import com.github.simpledatax.api.dto.DataPluginEnum;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.plugin.PluginResouce;
import com.github.simpledatax.plugin.rdbms.util.DataBaseType;

public class MySqlWriterParser implements Parser {

    @Override
    public Configuration parse(DataCollectPlugin plugin) throws DxException {
        MySqlDbWriter writer = null;
        if (!(plugin instanceof MySqlDbWriter)) {
            throw new DxException("插件对象转换异常，当前实例不是DBDataCollectWriter类型！");
        } else {
            writer = (MySqlDbWriter) plugin;
        }
        Configuration configuration = Configuration.newDefault();
        configuration.set("name", PluginResouce.RDBMS_WRITER.getName());
        configuration.set("parameter.username", writer.getDbUser());
        configuration.set("parameter.password", writer.getDbPassword());

        String cols = writer.getColumnStrs();
        List<String> result = Arrays.asList(cols.split(","));
        configuration.set("parameter.column", result);
        configuration.set("parameter.connection[0].table[0]", writer.getTableName());
        String jdbcUrl = MessageFormat.format("jdbc:mysql://{0}:{1}/{2}", writer.getDbIp(), writer.getDbPort(),
                writer.getDbInstanceName());
        configuration.set("parameter.connection[0].jdbcUrl", jdbcUrl);
        configuration.set("parameter.batchSize", "1024");
        return configuration;
    }

}
