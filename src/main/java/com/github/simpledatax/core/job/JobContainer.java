package com.github.simpledatax.core.job;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.github.simpledatax.api.adaptor.util.PropertiesKey;
import com.github.simpledatax.api.adaptor.util.ResourceUtil;
import com.github.simpledatax.common.constant.PluginType;
import com.github.simpledatax.common.exception.DataXException;
import com.github.simpledatax.common.plugin.AbstractJobPlugin;
import com.github.simpledatax.common.spi.Reader;
import com.github.simpledatax.common.spi.Writer;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.common.util.StrUtil;
import com.github.simpledatax.core.Engine;
import com.github.simpledatax.core.statistics.communication.Communication;
import com.github.simpledatax.core.statistics.communication.CommunicationTool;
import com.github.simpledatax.core.statistics.communication.Communicator;
import com.github.simpledatax.core.taskgroup.TaskGroupScheduler;
import com.github.simpledatax.core.util.ErrorRecordChecker;
import com.github.simpledatax.core.util.FrameworkErrorCode;
import com.github.simpledatax.core.util.container.CoreConstant;
import com.github.simpledatax.core.util.container.LoadUtil;
import com.github.simpledatax.dataxservice.face.domain.enums.State;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * job实例运行在jobContainer容器中，它是所有任务的master，负责初始化、拆分、调度、运行、回收、监控和汇报 但它并不做实际的数据同步操作
 */
public class JobContainer {
    private static final Logger LOG = LoggerFactory.getLogger(JobContainer.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected Configuration configuration;

    private long jobId;

    private boolean isClearJobTempFile;

    private String jobTempPath;

    private String readerPluginName;

    private String writerPluginName;

    /**
     * reader和writer jobContainer的实例
     */
    private Reader.Job jobReader;

    private Writer.Job jobWriter;

    private Configuration userConf;

    private long startTimeStamp;

    private long endTimeStamp;

    private int needChannelNumber;

    private int totalStage = 1;

    private Map<String, Object> resultMsg = new HashMap<String, Object>();

    private ErrorRecordChecker errorLimit;

    public JobContainer(Configuration configuration) {
        Validate.notNull(configuration, "Configuration can not be null.");
        this.configuration = configuration;
        errorLimit = new ErrorRecordChecker(configuration);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * jobContainer主要负责的工作全部在start()里面，包括init、prepare、split、scheduler、
     * post以及destroy和statistics
     */
    public void start() {
        LOG.info("DataX jobContainer starts job.");
        Communication totalCommunication=null;
        try {
            this.startTimeStamp = System.currentTimeMillis();
            // bak user origin config
            userConf = configuration.clone();
            LOG.debug("jobContainer starts to do init ...");
            this.init();
            LOG.info("jobContainer starts to do prepare ...");
            this.prepare();
            LOG.info("jobContainer starts to do split ...");
            this.totalStage = this.split();
            LOG.info("jobContainer starts to do schedule ...");
            totalCommunication = this.schedule();
            LOG.debug("jobContainer starts to do post ...");
            this.post();
            this.endTimeStamp = System.currentTimeMillis();
        } catch (Throwable e) {
            LOG.error("Exception when job run", e.getMessage());
            this.endTimeStamp = System.currentTimeMillis();
            if (e instanceof OutOfMemoryError) {
                this.destroy();
                System.gc();
            }
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        } finally {
            this.logStatistics(totalCommunication);
            this.destroy();
        }
    }

    /**
     * reader和writer的初始化
     */
    private void init() {
        this.jobId = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);
        this.isClearJobTempFile = this.configuration.getBool(CoreConstant.DATAX_CORE_CONTAINER_JOB_ISCLEARJOBTEMPFILE);
        this.createTempFile();
        if (this.jobId < 0) {
            LOG.info("Set jobId = 0");
            this.jobId = 0;
            this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, this.jobId);
        }

        // 必须先Reader ，后Writer
        this.jobReader = this.initJobReader();
        this.jobWriter = this.initJobWriter();
    }

    private void prepare() {
        this.prepareJobReader();
        this.prepareJobWriter();
    }

    /**
     * 创建临时文件
     */
    private void createTempFile() {
        String tempFilePath = this.configuration.getString(CoreConstant.DATAX_CORE_CONTAINER_JOB_TEMPFILEPATH);
        // warn: 这里用户需要配一个目录
        File dir = new File(tempFilePath);
        if (!dir.exists()) {
            boolean createdOk = dir.mkdirs();
            if (!createdOk) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        String.format("您指定的临时文件路径 : [%s] 创建失败.", tempFilePath));
            }
        }

        // 创建目标job文件路径
        String acceptId = (String) MDC.get(PropertiesKey.ACCEPT_ID);
        String jobFileName = acceptId == null ? UUID.randomUUID().toString() : acceptId;
        String jobDirName = StringUtils.join(new String[] { tempFilePath, jobFileName }, File.separator);
        this.jobTempPath = jobDirName;

        // 设置到插件中
        this.configuration.set("job.content[0].reader.parameter.jobTempPath", this.jobTempPath);
        this.configuration.set("job.content[0].writer.parameter.jobTempPath", this.jobTempPath);
    }

    private void preHandle() {
        String handlerPluginTypeStr = this.configuration.getString(CoreConstant.DATAX_JOB_PREHANDLER_PLUGINTYPE);
        if (!StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job preHandler's pluginType(%s) set error, reason(%s)",
                            handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName);

        // todo configuration的安全性，将来必须保证
        handler.preHandler(configuration);

        LOG.info("After PreHandler: \n" + Engine.filterJobConfiguration(configuration) + "\n");
    }

    private void postHandle() {
        String handlerPluginTypeStr = this.configuration.getString(CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINTYPE);

        if (!StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job postHandler's pluginType(%s) set error, reason(%s)",
                            handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName);

        handler.postHandler(configuration);
    }

    /**
     * 执行reader和writer最细粒度的切分，需要注意的是，writer的切分结果要参照reader的切分结果，
     * 达到切分后数目相等，才能满足1：1的通道模型，所以这里可以将reader和writer的配置整合到一起，
     * 然后，为避免顺序给读写端带来长尾影响，将整合的结果shuffler掉
     */
    private int split() {
        this.adjustChannelNumber();

        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }

        List<Configuration> readerTaskConfigs = this.doReaderSplit(this.needChannelNumber);
        int taskNumber = readerTaskConfigs.size();
        List<Configuration> writerTaskConfigs = this.doWriterSplit(taskNumber);

        List<Configuration> transformerList = this.configuration
                .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);

        LOG.debug("transformer configuration: " + JSON.toJSONString(transformerList));
        /**
         * 输入是reader和writer的parameter list，输出是content下面元素的list
         */
        List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(readerTaskConfigs, writerTaskConfigs,
                transformerList);

        LOG.debug("contentConfig configuration: " + JSON.toJSONString(contentConfig));

        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);

        return contentConfig.size();
    }

    private void adjustChannelNumber() {
        int channelNumber = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);
        if (channelNumber > 0) {
            this.needChannelNumber = channelNumber;
            return;
        }

        int needChannelNumberByByte = Integer.MAX_VALUE;
        int needChannelNumberByRecord = Integer.MAX_VALUE;

        boolean isByteLimit = (this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 0) > 0);
        if (isByteLimit) {
            long globalLimitedByteSpeed = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE,
                    10 * 1024 * 1024);

            // 在byte流控情况下，单个Channel流量最大值必须设置，否则报错！
            Long channelLimitedByteSpeed = this.configuration
                    .getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
                DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        "在有总bps限速条件下，单个channel的bps值不能为空，也不能为非正数");
            }

            needChannelNumberByByte = (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
            needChannelNumberByByte = needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
            LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
        }

        boolean isRecordLimit = (this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 0)) > 0;
        if (isRecordLimit) {
            long globalLimitedRecordSpeed = this.configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD,
                    100000);

            Long channelLimitedRecordSpeed = this.configuration
                    .getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
            if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
                DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        "在有总tps限速条件下，单个channel的tps值不能为空，也不能为非正数");
            }

            needChannelNumberByRecord = (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord = needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
            LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
        }

        // 取较小值
        this.needChannelNumber = needChannelNumberByByte < needChannelNumberByRecord ? needChannelNumberByByte
                : needChannelNumberByRecord;

        // 如果从byte或record上设置了needChannelNumber则退出
        if (this.needChannelNumber < Integer.MAX_VALUE) {
            return;
        }

        throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "Job运行速度必须设置");
    }

    /**
     * schedule首先完成的工作是把上一步reader和writer split的结果整合到具体taskGroup中,
     * 同时不同的执行模式调用不同的调度策略，将所有任务调度起来
     */
    private Communication schedule() {
        Communicator communicator = new Communicator();
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 0);
        TaskGroupScheduler scheduler = new TaskGroupScheduler(this.configuration, communicator);
        Communication totalCommunication = scheduler.schedule(this.needChannelNumber);
        this.checkLimit(totalCommunication);
        if (totalCommunication.getState() == State.SUCCEEDED) {
            LOG.info("Scheduler accomplished all tasks.");
        } else if (totalCommunication.getState() == State.FAILED) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
                    totalCommunication.getThrowable());
        }
        return totalCommunication;
    }

    private void post() {
        this.postJobWriter();
        this.postJobReader();
    }

    private void destroy() {
        if (this.jobWriter != null) {
            this.jobWriter.destroy();
            this.jobWriter = null;
        }
        if (this.jobReader != null) {
            this.jobReader.destroy();
            this.jobReader = null;
        }
        if (this.isClearJobTempFile) {
            File jobTempDir = new File(this.jobTempPath);
            if (jobTempDir.exists()) {
                try {
                    ResourceUtil.deleteFile(jobTempDir);
                } catch (Exception e) {
                    LOG.error("删除临时文件出现异常：{}。", this.jobTempPath);
                    e.printStackTrace();
                }
            }
        }
    }

    private void logStatistics(Communication totalCommunication) {
        if (totalCommunication == null) {
            return;
        }
        long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000;

        totalCommunication.setTimestamp(this.endTimeStamp);

        Communication tempComm = new Communication();
        tempComm.setTimestamp(this.startTimeStamp);

        Communication reportCommunication = CommunicationTool.getReportCommunication(totalCommunication, tempComm,
                this.totalStage);

        // 字节速率
        long byteSpeedPerSecond = totalCommunication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES) / totalCosts;

        long recordSpeedPerSecond = totalCommunication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS)
                / totalCosts;

        reportCommunication.setLongCounter(CommunicationTool.BYTE_SPEED, byteSpeedPerSecond);
        reportCommunication.setLongCounter(CommunicationTool.RECORD_SPEED, recordSpeedPerSecond);

        LOG.info(String.format(
                "\n" + "%-26s: %-18s\n" + "%-26s: %-18s\n" + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n" + "%-26s: %19s\n",
                "任务启动时刻", dateFormat.format(startTimeStamp),

                "任务结束时刻", dateFormat.format(endTimeStamp),

                "任务总计耗时", String.valueOf(totalCosts) + "s", "任务平均流量", StrUtil.stringify(byteSpeedPerSecond) + "/s",
                "记录写入速度", String.valueOf(recordSpeedPerSecond) + "rec/s", "读出记录总数",
                String.valueOf(CommunicationTool.getTotalReadRecords(totalCommunication)), "读写失败总数",
                String.valueOf(CommunicationTool.getTotalErrorRecords(totalCommunication))));

        // 写入到结果对象中
        resultMsg.put("startTime", dateFormat.format(startTimeStamp));
        resultMsg.put("endTime", dateFormat.format(endTimeStamp));
        resultMsg.put("totalCosts", totalCosts);
        resultMsg.put("byteSpeedPerSecond", byteSpeedPerSecond);
        resultMsg.put("recordSpeedPerSecond", recordSpeedPerSecond);
        resultMsg.put("totalReadRecords", CommunicationTool.getTotalReadRecords(totalCommunication));
        resultMsg.put("totalErrorRecords", CommunicationTool.getTotalErrorRecords(totalCommunication));

        if (totalCommunication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS) > 0
                || totalCommunication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS) > 0
                || totalCommunication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS) > 0) {
            LOG.info(String.format("\n" + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n", "Transformer成功记录总数",
                    totalCommunication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS),

                    "Transformer失败记录总数",
                    totalCommunication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS),

                    "Transformer过滤记录总数",
                    totalCommunication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS)));
        }
    }

    /**
     * reader job的初始化，返回Reader.Job
     *
     * @return
     */
    private Reader.Job initJobReader() {
        this.readerPluginName = this.configuration.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName);
        // 设置reader的param
        jobReader
                .setPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        // 设置writer的param
        jobReader.setPeerPluginJobConf(
                this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

        jobReader.init();
        return jobReader;
    }

    /**
     * writer job的初始化，返回Writer.Job
     *
     * @return
     */
    private Writer.Job initJobWriter() {
        this.writerPluginName = this.configuration.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName);

        // 设置writer的param
        jobWriter
                .setPluginJobConf(this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

        // 设置reader的param
        jobWriter.setPeerPluginJobConf(
                this.configuration.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);

        jobWriter.init();
        return jobWriter;
    }

    private void prepareJobReader() {
        LOG.info(String.format("DataX Reader.Job [%s] do prepare work .", this.readerPluginName));
        this.jobReader.prepare();
    }

    private void prepareJobWriter() {
        LOG.info(String.format("DataX Writer.Job [%s] do prepare work .", this.writerPluginName));
        this.jobWriter.prepare();
    }

    // TODO: 如果源头就是空数据
    private List<Configuration> doReaderSplit(int adviceNumber) {
        List<Configuration> readerSlicesConfigs = this.jobReader.split(adviceNumber);
        if (readerSlicesConfigs == null || readerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "reader切分的task数目不能小于等于0");
        }
        LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.", this.readerPluginName, readerSlicesConfigs.size());
        return readerSlicesConfigs;
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber) {

        List<Configuration> writerSlicesConfigs = this.jobWriter.split(readerTaskNumber);
        if (writerSlicesConfigs == null || writerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "writer切分的task不能小于等于0");
        }
        LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.", this.writerPluginName, writerSlicesConfigs.size());

        return writerSlicesConfigs;
    }

    /**
     * 按顺序整合reader和writer的配置，这里的顺序不能乱！ 输入是reader、writer级别的配置，输出是一个完整task的配置
     *
     * @param readerTasksConfigs
     * @param writerTasksConfigs
     * @return
     */
    private List<Configuration> mergeReaderAndWriterTaskConfigs(List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs) {
        return mergeReaderAndWriterTaskConfigs(readerTasksConfigs, writerTasksConfigs, null);
    }

    private List<Configuration> mergeReaderAndWriterTaskConfigs(List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs, List<Configuration> transformerConfigs) {
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].", readerTasksConfigs.size(),
                            writerTasksConfigs.size()));
        }

        List<Configuration> contentConfigs = new ArrayList<Configuration>();
        for (int i = 0; i < readerTasksConfigs.size(); i++) {
            Configuration taskConfig = Configuration.newDefault();
            taskConfig.set(CoreConstant.JOB_READER_NAME, this.readerPluginName);
            taskConfig.set(CoreConstant.JOB_READER_PARAMETER, readerTasksConfigs.get(i));
            taskConfig.set(CoreConstant.JOB_WRITER_NAME, this.writerPluginName);
            taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER, writerTasksConfigs.get(i));

            if (transformerConfigs != null && transformerConfigs.size() > 0) {
                taskConfig.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
            }

            taskConfig.set(CoreConstant.TASK_ID, i);
            contentConfigs.add(taskConfig);
        }

        return contentConfigs;
    }

    /**
     * 这里比较复杂，分两步整合 1. tasks到channel 2. channel到taskGroup
     * 合起来考虑，其实就是把tasks整合到taskGroup中，需要满足计算出的channel数，同时不能多起channel
     * <p/>
     * example:
     * <p/>
     * 前提条件： 切分后是1024个分表，假设用户要求总速率是1000M/s，每个channel的速率的3M/s，
     * 每个taskGroup负责运行7个channel
     * <p/>
     * 计算： 总channel数为：1000M/s / 3M/s =
     * 333个，为平均分配，计算可知有308个每个channel有3个tasks，而有25个每个channel有4个tasks，
     * 需要的taskGroup数为：333 / 7 =
     * 47...4，也就是需要48个taskGroup，47个是每个负责7个channel，有4个负责1个channel
     * <p/>
     * 处理：我们先将这负责4个channel的taskGroup处理掉，逻辑是： 先按平均为3个tasks找4个channel，设置taskGroupId为0，
     * 接下来就像发牌一样轮询分配task到剩下的包含平均channel数的taskGroup中
     * <p/>
     * TODO delete it
     *
     * @param averTaskPerChannel
     * @param channelNumber
     * @param channelsPerTaskGroup
     * @return 每个taskGroup独立的全部配置
     */
    @SuppressWarnings("serial")
    private List<Configuration> distributeTasksToTaskGroup(int averTaskPerChannel, int channelNumber,
            int channelsPerTaskGroup) {
        Validate.isTrue(averTaskPerChannel > 0 && channelNumber > 0 && channelsPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");
        List<Configuration> taskConfigs = this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        int taskGroupNumber = channelNumber / channelsPerTaskGroup;
        int leftChannelNumber = channelNumber % channelsPerTaskGroup;
        if (leftChannelNumber > 0) {
            taskGroupNumber += 1;
        }

        /**
         * 如果只有一个taskGroup，直接打标返回
         */
        if (taskGroupNumber == 1) {
            final Configuration taskGroupConfig = this.configuration.clone();
            /**
             * configure的clone不能clone出
             */
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT,
                    this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT));
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, channelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 0);
            return new ArrayList<Configuration>() {
                {
                    add(taskGroupConfig);
                }
            };
        }

        List<Configuration> taskGroupConfigs = new ArrayList<Configuration>();
        /**
         * 将每个taskGroup中content的配置清空
         */
        for (int i = 0; i < taskGroupNumber; i++) {
            Configuration taskGroupConfig = this.configuration.clone();
            List<Configuration> taskGroupJobContent = taskGroupConfig
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
            taskGroupJobContent.clear();
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);

            taskGroupConfigs.add(taskGroupConfig);
        }

        int taskConfigIndex = 0;
        int channelIndex = 0;
        int taskGroupConfigIndex = 0;

        /**
         * 先处理掉taskGroup包含channel数不是平均值的taskGroup
         */
        if (leftChannelNumber > 0) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            for (; channelIndex < leftChannelNumber; channelIndex++) {
                for (int i = 0; i < averTaskPerChannel; i++) {
                    List<Configuration> taskGroupJobContent = taskGroupConfig
                            .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                    taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                    taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);
                }
            }

            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, leftChannelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, taskGroupConfigIndex++);
        }

        /**
         * 下面需要轮询分配，并打上channel数和taskGroupId标记
         */
        int equalDivisionStartIndex = taskGroupConfigIndex;
        for (; taskConfigIndex < taskConfigs.size() && equalDivisionStartIndex < taskGroupConfigs.size();) {
            for (taskGroupConfigIndex = equalDivisionStartIndex; taskGroupConfigIndex < taskGroupConfigs.size()
                    && taskConfigIndex < taskConfigs.size(); taskGroupConfigIndex++) {
                Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
                List<Configuration> taskGroupJobContent = taskGroupConfig
                        .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);
            }
        }

        for (taskGroupConfigIndex = equalDivisionStartIndex; taskGroupConfigIndex < taskGroupConfigs.size();) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, channelsPerTaskGroup);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, taskGroupConfigIndex++);
        }

        return taskGroupConfigs;
    }

    private void postJobReader() {
        LOG.info("DataX Reader.Job [{}] do post work.", this.readerPluginName);
        this.jobReader.post();
    }

    private void postJobWriter() {
        LOG.info("DataX Writer.Job [{}] do post work.", this.writerPluginName);
        this.jobWriter.post();
    }

    /**
     * 检查最终结果是否超出阈值，如果阈值设定小于1，则表示百分数阈值，大于1表示条数阈值。
     *
     * @param
     */
    private void checkLimit(Communication communication) {
        errorLimit.checkRecordLimit(communication);
        errorLimit.checkPercentageLimit(communication);
    }

    public Map<String, Object> getResultMsg() {
        return resultMsg;
    }

    public void setResultMsg(Map<String, Object> resultMsg) {
        this.resultMsg = resultMsg;
    }
}
