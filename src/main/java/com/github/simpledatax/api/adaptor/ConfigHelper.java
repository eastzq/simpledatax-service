package com.github.simpledatax.api.adaptor;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.simpledatax.api.adaptor.exception.DxException;
import com.github.simpledatax.api.adaptor.parser.intf.Parser;
import com.github.simpledatax.api.adaptor.parser.reader.MySqlReaderParser;
import com.github.simpledatax.api.adaptor.parser.writer.MySqlWriterParser;
import com.github.simpledatax.api.adaptor.util.ResourceUtil;
import com.github.simpledatax.api.dto.DataCollectJob;
import com.github.simpledatax.api.dto.DataCollectReader;
import com.github.simpledatax.api.dto.DataCollectWriter;
import com.github.simpledatax.api.dto.DataPluginEnum;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.core.util.container.CoreConstant;

/**
 * 配置对象初始化类！
 * 
 * @author zq
 *
 */
public class ConfigHelper {

    private static Logger logger = LoggerFactory.getLogger(ConfigHelper.class);
    /**
     * 插件配置存放位置！
     */
    public static Configuration plugins;

    /**
     * 加载基础架子和一些默认配置
     */
    public static String BASE_JSON;
    public static String CORE_JSON;

    /**
     * 存放各种插件配置。默认预加载各种插件！
     */
    static {
        initConfigurer();
    }

    /**
     * 撘建架子，将输入参数转换成datax默认的配置对象。
     * 
     * @param job
     * @return
     * @throws DxException
     */
    public static Configuration parseJob(DataCollectJob job) throws DxException {
        Configuration mainConf = Configuration.from(CORE_JSON);
        mainConf.merge(Configuration.from(ConfigHelper.BASE_JSON), false);
        mainConf.merge(plugins, false);
        // 控制并发数
        if (job.getChannelNum() > 0) {
            mainConf.set(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CHANNELNUM, job.getChannelNum());
        }
        mainConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ISCLEARJOBTEMPFILE, job.isClearJobTempFile());
        Configuration readerConf = parseReader(job.getReader());
        Configuration writerConf = parseWriter(job.getWriter());
        mainConf.set(CoreConstant.DATAX_JOB_CONTENT_READER, readerConf.getInternal());
        mainConf.set(CoreConstant.DATAX_JOB_CONTENT_WRITER, writerConf.getInternal());
        return mainConf;
    }
    
    /**
     * 撘建架子，将输入参数转换成datax默认的配置对象。
     * 
     * @param job
     * @return
     * @throws DxException
     */
    public static Configuration parseJob(String baseJson,int channelNum) throws DxException {
        Configuration mainConf = Configuration.from(CORE_JSON);
        mainConf.merge(Configuration.from(baseJson), false);
        mainConf.merge(plugins, false);
        mainConf.set(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CHANNELNUM, channelNum);
        return mainConf;
    }
    
    /**
     * 初始化datax配置
     */
    private static void initConfigurer() {
        logger.info("初始化采集模块配置！");
        plugins = Configuration.newDefault();
        InputStream is1 = null;
        InputStream is2 = null;
        try {
            is1 = ResourceUtil.getResourceInJar(ConfigHelper.class, "config/base.json");
            is2 = ResourceUtil.getResourceInJar(ConfigHelper.class, "config/core.json");
            BASE_JSON = IOUtils.toString(is1, "utf-8");
            CORE_JSON = IOUtils.toString(is2, "utf-8");
            // 注册插件
            PluginResouce[] plugins = PluginResouce.values();
            for (int i = 0; i < plugins.length; i++) {
                PluginResouce plugin = plugins[i];
                registerPlugin(plugin.getType(), plugin.getName(), plugin.getClassName(), plugin.getDesc());
            }
        } catch (Exception e) {
            logger.error("初始化采集模块配置失败，ConfigHelper类初始化失败，请检查配置！");
            throw new RuntimeException("读取配置文件出现异常，请检查目标目录是否存在配置文件！", e);
        } finally {
            IOUtils.closeQuietly(is1);
            IOUtils.closeQuietly(is2);
        }
    }

    /**
     * 注册reader和writer插件
     */
    private static void registerPlugin(String pluginType, String pluginName, String pluginClass, String pluginDesc) {
        Configuration temp = Configuration.newDefault();
        temp.set("class", pluginClass);
        temp.set("name", pluginName);
        temp.set("desc", pluginClass == null ? "" : pluginDesc);
        plugins.set(String.format("plugin.%s.%s", pluginType, pluginName), temp.getInternal());
    }

    // 转换reader
    private static Configuration parseReader(DataCollectReader reader) throws DxException {
        Parser parser = null;
        if (reader.getReaderType() == DataPluginEnum.RMDBS) {
            parser = new MySqlReaderParser();
            return parser.parse(reader);
        } else {
            throw new DxException("找不到对应的parser！");
        }
    }

    // 转换writer
    private static Configuration parseWriter(DataCollectWriter writer) throws DxException {
        Parser parser = null;
        if (writer.getWriterType() == DataPluginEnum.RMDBS) {
            parser = new MySqlWriterParser();
            return parser.parse(writer);
        } else {
            throw new DxException("找不到对应的parser！");
        }
    }

}
