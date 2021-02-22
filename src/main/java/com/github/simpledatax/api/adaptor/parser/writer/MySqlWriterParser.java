package com.github.simpledatax.api.adaptor.parser.writer;

import com.github.simpledatax.api.adaptor.exception.DxException;
import com.github.simpledatax.api.adaptor.parser.intf.Parser;
import com.github.simpledatax.api.dto.DataCollectPlugin;
import com.github.simpledatax.api.dto.writer.MySqlWriter;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.plugin.PluginResouce;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

public class MySqlWriterParser implements Parser {

    @Override
    public Configuration parse(DataCollectPlugin plugin) throws DxException {
        MySqlWriter writer = null;
        if (!(plugin instanceof MySqlWriter)) {
            throw new DxException("插件对象转换异常，当前实例不是MySqlWriter类型！");
        } else {
            writer = (MySqlWriter) plugin;
        }
        Configuration configuration = Configuration.newDefault();
        configuration.set("name", PluginResouce.MYSQL_WRITER.getName());
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
