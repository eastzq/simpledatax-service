package com.github.simpledatax.api.adaptor;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.simpledatax.api.adaptor.exception.DxException;
import com.github.simpledatax.api.adaptor.parser.intf.Parser;
import com.github.simpledatax.api.adaptor.parser.reader.MySqlReaderParser;
import com.github.simpledatax.api.adaptor.parser.writer.MySqlWriterParser;
import com.github.simpledatax.api.adaptor.util.PropertiesKey;
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
    private static Properties conf;

    /**
     * 加载基础架子和一些默认配置
     */
    public static String BASE_JSON;
    public static String CORE_JSON;

    /**
     * 存放各种插件配置。默认预加载各种插件！
     */
    static {

        initDataxConfigurer();
        initConfigurer();
    }

    private static void initConfigurer() {
        logger.info("初始化全局配置对象！");
        // 默认从web容器里取
        ConfigHelper.conf = new Properties();
        try {
            conf.load(ResourceUtil.getResourceInJar(ConfigHelper.class, "conf.properties"));
        } catch (Exception e) {
            throw new RuntimeException("初始化全局配置对象失败！");
        }
    }

    /**
     * 获取全局配置属性
     * 
     * @throws Exception
     */
    public static String getConfigValue(String key) {
        String result = "";
        try {
            result = conf.getProperty(key);
        } catch (Exception e) {
            logger.error("从epbos全局配置里获取配置失败，key：{}", key);
        }
        return result;
    }

    /**
     * 撘建架子，将输入参数转换成datax默认的配置对象。
     * 
     * @param job
     * @return
     * @throws DxException
     */
    public static Configuration parseJob(DataCollectJob job) throws DxException {
        Configuration mainConf = Configuration.from(BASE_JSON);
        // 控制并发数
        if (job.getChannelNum() > 0) {
            mainConf.set("job.setting.speed.channel", job.getChannelNum());
        }
        // 临时文件配置
        String tempFilePath = "D:/test";
        if (StringUtils.isBlank(tempFilePath)) {
            throw new DxException("从全局配置文件中获取临时文件目录失败，属性名称: " + PropertiesKey.TEMP_FILE_PATH);
        }
        mainConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ISCLEARJOBTEMPFILE, job.isClearJobTempFile());
        mainConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_TEMPFILEPATH, tempFilePath);

        Configuration readerConf = parseReader(job.getReader());
        Configuration writerConf = parseWriter(job.getWriter());
        mainConf.set("job.content[0].reader", readerConf.getInternal());
        mainConf.set("job.content[0].writer", writerConf.getInternal());
        mainConf.merge(plugins, false);
        return mainConf;
    }

    /**
     * 初始化datax配置
     */
    private static void initDataxConfigurer() {
        logger.info("初始化采集模块配置！");
        plugins = Configuration.newDefault();
        InputStream is1 = null;
        InputStream is2 = null;
        try {
            is1 = ResourceUtil.getResourceInJar(ConfigHelper.class, "datax/conf/base.json");
            is2 = ResourceUtil.getResourceInJar(ConfigHelper.class, "datax/conf/core.json");
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
