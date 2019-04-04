package com.zq.simpledatax.plugin.reader.oraclereader;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zq.simpledatax.common.exception.DataXException;
import com.zq.simpledatax.common.plugin.RecordSender;
import com.zq.simpledatax.common.spi.Reader;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.plugin.rdbms.reader.CommonRdbmsReader;
import com.zq.simpledatax.plugin.rdbms.reader.RdbmsReaderKey;
import com.zq.simpledatax.plugin.rdbms.util.DBUtilErrorCode;
import com.zq.simpledatax.plugin.rdbms.util.DataBaseType;

public class OracleReader extends Reader {

	private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

	public static class Job extends Reader.Job {

        private static final Logger logger = LoggerFactory.getLogger(OracleReader.Job.class);

		private Configuration originalConfig = null;
		private CommonRdbmsReader.Job commonRdbmsReaderJob;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			
			dealFetchSize(this.originalConfig);

			this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(
					DATABASE_TYPE);
			this.commonRdbmsReaderJob.init(this.originalConfig);

			// 注意：要在 this.commonRdbmsReaderJob.init(this.originalConfig); 之后执行，这样可以直接快速判断是否是querySql 模式
			dealHint(this.originalConfig);
		}

        @Override
        public void preCheck(){
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);
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

		private void dealFetchSize(Configuration originalConfig) {
			int fetchSize = originalConfig.getInt(
					com.zq.simpledatax.plugin.rdbms.reader.RdbmsReaderConstant.FETCH_SIZE,
					Constant.DEFAULT_FETCH_SIZE);
			if (fetchSize < 1) {
				throw DataXException
						.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
								String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
										fetchSize));
			}
			originalConfig.set(
					com.zq.simpledatax.plugin.rdbms.reader.RdbmsReaderConstant.FETCH_SIZE,
					fetchSize);
		}

		private void dealHint(Configuration originalConfig) {
			String hint = originalConfig.getString(RdbmsReaderKey.HINT);
			if (StringUtils.isNotBlank(hint)) {
				boolean isTableMode = originalConfig.getBool(com.zq.simpledatax.plugin.rdbms.reader.RdbmsReaderConstant.IS_TABLE_MODE).booleanValue();
				if(!isTableMode){
					throw DataXException.asDataXException(OracleReaderErrorCode.HINT_ERROR, "当且仅当非 querySql 模式读取 oracle 时才能配置 HINT.");
				}
			}
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
					.getInt(com.zq.simpledatax.plugin.rdbms.reader.RdbmsReaderConstant.FETCH_SIZE);

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
