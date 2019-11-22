package com.github.simpledatax.plugin.reader.sqlserverreader;

import java.util.List;

import com.github.simpledatax.common.exception.DataXException;
import com.github.simpledatax.common.plugin.RecordSender;
import com.github.simpledatax.common.spi.Reader;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.plugin.rdbms.reader.CommonRdbmsReader;
import com.github.simpledatax.plugin.rdbms.reader.RdbmsReaderConstant;
import com.github.simpledatax.plugin.rdbms.util.DBUtilErrorCode;
import com.github.simpledatax.plugin.rdbms.util.DataBaseType;

public class SqlServerReader extends Reader {

	private static final DataBaseType DATABASE_TYPE = DataBaseType.SQLServer;

	public static class Job extends Reader.Job {

		private Configuration originalConfig = null;
		private CommonRdbmsReader.Job commonRdbmsReaderJob;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			int fetchSize = this.originalConfig.getInt(
					RdbmsReaderConstant.FETCH_SIZE,
					Constant.DEFAULT_FETCH_SIZE);
			if (fetchSize < 1) {
				throw DataXException
						.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
								String.format("您配置的fetchSize有误，根据DataX的设计，fetchSize : [%d] 设置值不能小于 1.",
										fetchSize));
			}
			this.originalConfig.set(
					RdbmsReaderConstant.FETCH_SIZE,
					fetchSize);

			this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(
					DATABASE_TYPE);
			this.commonRdbmsReaderJob.init(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			return this.commonRdbmsReaderJob.split(this.originalConfig,
					adviceNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsReaderJob.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsReaderJob.destroy(this.originalConfig);
		}

	}

	public static class Task extends Reader.Task {

		private Configuration readerSliceConfig;
		private CommonRdbmsReader.Task commonRdbmsReaderTask;

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(
					DATABASE_TYPE ,super.getTaskGroupId(), super.getTaskId());
			this.commonRdbmsReaderTask.init(this.readerSliceConfig);
		}

		@Override
		public void startRead(RecordSender recordSender) {
			int fetchSize = this.readerSliceConfig
					.getInt(RdbmsReaderConstant.FETCH_SIZE);

			this.commonRdbmsReaderTask.startRead(this.readerSliceConfig,
					recordSender, super.getTaskPluginCollector(), fetchSize);
		}

		@Override
		public void post() {
			this.commonRdbmsReaderTask.post(this.readerSliceConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
		}

	}

}
