package com.zq.simpledatax.plugin.writer.rdbmswriter;

import java.util.List;

import com.zq.simpledatax.common.exception.DataXException;
import com.zq.simpledatax.common.plugin.RecordReceiver;
import com.zq.simpledatax.common.spi.Writer;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.plugin.rdbms.util.DBUtilErrorCode;
import com.zq.simpledatax.plugin.rdbms.util.DataBaseType;
import com.zq.simpledatax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.zq.simpledatax.plugin.rdbms.writer.RdbmsWriterKey;

public class RdbmsWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.RDBMS;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            // warn：not like mysql, only support insert mode, don't use
            String writeMode = this.originalConfig.getString(RdbmsWriterKey.WRITE_MODE);
            if (null != writeMode) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.CONF_ERROR,
                                String.format(
                                        "写入模式(writeMode)配置有误. 因为不支持配置参数项 writeMode: %s, 仅使用insert sql 插入数据. 请检查您的配置并作出修改.",
                                        writeMode));
            }

            this.commonRdbmsWriterMaster = new SubCommonRdbmsWriter.Job(
                    DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsWriterMaster.split(this.originalConfig,
                    mandatoryNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsWriterMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterSlave = new SubCommonRdbmsWriter.Task(
                    DATABASE_TYPE);
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterSlave.startWrite(recordReceiver,
                    this.writerSliceConfig, super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }

    }

}