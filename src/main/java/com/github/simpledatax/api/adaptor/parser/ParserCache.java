package com.github.simpledatax.api.adaptor.parser;

import java.util.HashMap;
import java.util.Map;

import com.github.simpledatax.api.adaptor.exception.DxException;
import com.github.simpledatax.api.adaptor.parser.intf.Parser;
import com.github.simpledatax.api.adaptor.parser.reader.MySqlReaderParser;
import com.github.simpledatax.api.adaptor.parser.writer.MySqlWriterParser;
import com.github.simpledatax.api.dto.DataCollectPlugin;
import com.github.simpledatax.api.dto.DataPluginEnum;
import com.github.simpledatax.common.util.Configuration;

public class ParserCache {

    public static Map<DataPluginEnum, Class<? extends Parser>> parserMapper = new HashMap<DataPluginEnum, Class<? extends Parser>>();

    static {
        parserMapper.put(DataPluginEnum.MYSQL_READER, MySqlReaderParser.class);
        parserMapper.put(DataPluginEnum.MYSQL_WRITER, MySqlWriterParser.class);
    }

    public static Configuration parse(DataCollectPlugin plugin) throws InstantiationException, IllegalAccessException {
        Class<? extends Parser> clazz = parserMapper.get(plugin.getPluginName());
        if (clazz == null) {
            throw new DxException("未找到对应的转换器" + plugin.getPluginName().getType());
        }
        Parser parser = clazz.newInstance();
        return parser.parse(plugin);
    }

}
