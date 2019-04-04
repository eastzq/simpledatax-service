package com.zq.simpledatax.plugin.writer.oraclewriter;

import java.util.List;

import com.zq.simpledatax.common.exception.DataXException;
import com.zq.simpledatax.common.plugin.RecordReceiver;
import com.zq.simpledatax.common.spi.Writer;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.plugin.rdbms.util.DBUtilErrorCode;
import com.zq.simpledatax.plugin.rdbms.util.DataBaseType;
import com.zq.simpledatax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.zq.simpledatax.plugin.rdbms.writer.RdbmsWriterKey;

public class OracleWriter extends Writer {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        public void preCheck() {
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();

			// warn：not like mysql, oracle only support insert mode, don't use
			String writeMode = this.originalConfig.getString(RdbmsWriterKey.WRITE_MODE);
			if (null != writeMode) {
				throw DataXException
						.asDataXException(
								DBUtilErrorCode.CONF_ERROR,
								String.format(
										"写入模式(writeMode)配置错误. 因为Oracle不支持配置项 writeMode: %s, Oracle只能使用insert sql 插入数据. 请检查您的配置并作出修改",
										writeMode));
			}

            this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
			this.commonRdbmsWriterJob.init(this.originalConfig);
		}

		@Override
		public void prepare() {
            //oracle实跑先不做权限检查
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
			this.commonRdbmsWriterJob.prepare(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return this.commonRdbmsWriterJob.split(this.originalConfig,
					mandatoryNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsWriterJob.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterJob.destroy(this.originalConfig);
		}

	}

	public static class Task extends Writer.Task {
		private Configuration writerSliceConfig;
		private CommonRdbmsWriter.Task commonRdbmsWriterTask;

		@Override
		public void init() {
			this.writerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
			this.commonRdbmsWriterTask.init(this.writerSliceConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
		}

		public void startWrite(RecordReceiver recordReceiver) {
			this.commonRdbmsWriterTask.startWrite(recordReceiver,
					this.writerSliceConfig, super.getTaskPluginCollector());
		}

		@Override
		public void post() {
			this.commonRdbmsWriterTask.post(this.writerSliceConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
		}

	}

}
