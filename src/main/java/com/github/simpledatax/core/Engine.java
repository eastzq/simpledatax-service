package com.github.simpledatax.core;

import com.alibaba.fastjson.JSON;
import com.github.simpledatax.api.dto.DataCollectResult;
import com.github.simpledatax.common.element.ColumnCast;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.core.job.JobContainer;
import com.github.simpledatax.core.util.ConfigurationValidate;
import com.github.simpledatax.core.util.SecretUtil;
import com.github.simpledatax.core.util.container.CoreConstant;
import com.github.simpledatax.core.util.container.LoadUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine {
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    private static String RUNTIME_MODE = "standalone";

    private long jobId;

    private Configuration allConfiguration;

    public Engine(long jobId, Configuration allConfiguration) {
        super();
        this.jobId = jobId;
        this.allConfiguration = allConfiguration;
    }

    /* check job model (job/task) first */
    public DataCollectResult start() throws Exception {

        this.allConfiguration = SecretUtil.decryptSecretKey(allConfiguration);

        allConfiguration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);
        // 打印配置信息
        LOG.info("\n" + Engine.filterJobConfiguration(allConfiguration) + "\n");
        ConfigurationValidate.doValidate(allConfiguration);

        Map<String, Object> result = null;
        // 绑定column转换信息
        ColumnCast.bind(allConfiguration);
        // 初始化PluginLoader，可以获取各种插件配置
        LoadUtil.bind(allConfiguration);
        allConfiguration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, RUNTIME_MODE);
        LOG.info("\n" + JSON.toJSONString(allConfiguration) + "\n");
        JobContainer container = new JobContainer(allConfiguration);
        container.start();
        result = ((JobContainer) container).getResultMsg();
        DataCollectResult data = new DataCollectResult();
        BeanUtils.populate(data, result);
        
        return data;
    }

    // 注意屏蔽敏感信息
    public static String filterJobConfiguration(final Configuration configuration) {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();

        Configuration jobContent = jobConfWithSetting.getConfiguration("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content", jobContent);

        return jobConfWithSetting.beautify();
    }

    public static Configuration filterSensitiveConfiguration(Configuration configuration) {
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }
        return configuration;
    }

    public static DataCollectResult run(long jobId, Configuration configuration) throws Exception {
        return new Engine(jobId, configuration).start();
    }
}
