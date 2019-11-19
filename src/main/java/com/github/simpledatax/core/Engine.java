package com.github.simpledatax.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.simpledatax.api.dto.DataCollectResult;
import com.github.simpledatax.common.element.ColumnCast;
import com.github.simpledatax.common.exception.DataXException;
import com.github.simpledatax.common.spi.ErrorCode;
import com.github.simpledatax.common.statistics.VMInfo;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.core.job.JobContainer;
import com.github.simpledatax.core.util.ConfigParser;
import com.github.simpledatax.core.util.ConfigurationValidate;
import com.github.simpledatax.core.util.ExceptionTracker;
import com.github.simpledatax.core.util.FrameworkErrorCode;
import com.github.simpledatax.core.util.container.CoreConstant;
import com.github.simpledatax.core.util.container.LoadUtil;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine {
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    private static String RUNTIME_MODE = "standalone";

    /* check job model (job/task) first */
    public Map<String, Object> start(Configuration allConf) {
        Map<String, Object> result = null;
        // 绑定column转换信息
        ColumnCast.bind(allConf);
        // 初始化PluginLoader，可以获取各种插件配置
        LoadUtil.bind(allConf);
        allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, RUNTIME_MODE);
        JobContainer container = new JobContainer(allConf);
        container.start();
        result = ((JobContainer) container).getResultMsg();
        return result;
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

    public static void entry(final String[] args) throws Throwable {
        Options options = new Options();
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("mode", true, "Job runtime mode.");

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);

        String jobPath = cl.getOptionValue("job");

        // 如果用户没有明确指定jobid, 则 datax.py 会指定 jobid 默认值为-1
        String jobIdString = cl.getOptionValue("jobid");
        RUNTIME_MODE = cl.getOptionValue("mode");

        Configuration configuration = ConfigParser.parse(jobPath);

        long jobId;
        if (!"-1".equalsIgnoreCase(jobIdString)) {
            jobId = Long.parseLong(jobIdString);
        } else {
            // only for dsc & ds & datax 3 update
            String dscJobUrlPatternString = "/instance/(\\d{1,})/config.xml";
            String dsJobUrlPatternString = "/inner/job/(\\d{1,})/config";
            String dsTaskGroupUrlPatternString = "/inner/job/(\\d{1,})/taskGroup/";
            List<String> patternStringList = Arrays.asList(dscJobUrlPatternString, dsJobUrlPatternString,
                    dsTaskGroupUrlPatternString);
            jobId = parseJobIdFromUrl(patternStringList, jobPath);
        }

        boolean isStandAloneMode = "standalone".equalsIgnoreCase(RUNTIME_MODE);
        if (!isStandAloneMode && jobId == -1) {
            // 如果不是 standalone 模式，那么 jobId 一定不能为-1
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    "非 standalone 模式必须在 URL 中提供有效的 jobId.");
        }
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);

        // 打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            LOG.info(vmInfo.toString());
        }

        LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");

        LOG.debug(configuration.toJSON());

        ConfigurationValidate.doValidate(configuration);
        Engine engine = new Engine();
        engine.start(configuration);
    }

    /**
     * -1 表示未能解析到 jobId
     *
     * only for dsc & ds & datax 3 update
     */
    private static long parseJobIdFromUrl(List<String> patternStringList, String url) {
        long result = -1;
        for (String patternString : patternStringList) {
            result = doParseJobIdFromUrl(patternString, url);
            if (result != -1) {
                return result;
            }
        }
        return result;
    }

    private static long doParseJobIdFromUrl(String patternString, String url) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }

        return -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        try {
            Engine.entry(args);
        } catch (Throwable e) {
            exitCode = 1;
            LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(e));

            if (e instanceof DataXException) {
                DataXException tempException = (DataXException) e;
                ErrorCode errorCode = tempException.getErrorCode();
                if (errorCode instanceof FrameworkErrorCode) {
                    FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
                    exitCode = tempErrorCode.toExitValue();
                }
            }

            System.exit(exitCode);
        }
        System.exit(exitCode);
    }

    /**
     * 新建程序入口
     */
    public static Map run(String[] args) throws Throwable {
        try {
            for (String arg : args) {
                LOG.info("入参-------->" + arg.toString());
            }
            // Engine.xy_entry(args);
        } catch (Throwable e) {
            LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(e));
            throw e;
        }
        return null;
    }

    public static DataCollectResult xy_entry(long jobId, Configuration configuration) throws Throwable {
        configuration = ConfigParser.xy_parse(configuration);
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);
        // 打印配置信息
        LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");
        ConfigurationValidate.doValidate(configuration);
        Engine engine = new Engine();
        Map<String, Object> resultMsg = engine.start(configuration);
        DataCollectResult data = new DataCollectResult();
        BeanUtils.populate(data, resultMsg);
        return data;
    }
}
