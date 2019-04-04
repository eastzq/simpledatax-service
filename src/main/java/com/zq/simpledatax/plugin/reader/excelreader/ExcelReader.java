package com.zq.simpledatax.plugin.reader.excelreader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zq.simpledatax.common.element.Column;
import com.zq.simpledatax.common.element.Record;
import com.zq.simpledatax.common.element.StringColumn;
import com.zq.simpledatax.common.exception.DataXException;
import com.zq.simpledatax.common.plugin.RecordSender;
import com.zq.simpledatax.common.spi.Reader;
import com.zq.simpledatax.common.util.Configuration;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class ExcelReader extends Reader {
	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration jobConfig = null;

		@Override
		public void init() {
			this.jobConfig = this.getPluginJobConf();
			this.validateParameter();
		}

		private void validateParameter() {
			LOG.info("开始校验传入的参数！");
			String filePath = this.jobConfig.getNecessaryValue(Key.PATH, ExcelReaderErrorCode.REQUIRED_VALUE);
			String column = this.jobConfig.getNecessaryValue(Key.COLUMN, ExcelReaderErrorCode.REQUIRED_VALUE);
			String dataSheets = this.jobConfig.getNecessaryValue(Key.DATA_SHEETS, ExcelReaderErrorCode.REQUIRED_VALUE);
			
			File file = new File(filePath);
			if (!file.exists()) {
				throw DataXException.asDataXException(ExcelReaderErrorCode.FILE_NOT_EXISTS,String.format("文件 : [%s]", filePath));
			}
			String extension = filePath.substring(filePath.lastIndexOf(".")+1);
			if(!extension.equalsIgnoreCase("xls") && !extension.equalsIgnoreCase("xlsx")) {
				throw DataXException.asDataXException(ExcelReaderErrorCode.FILE_TYPE_ERROR,String.format("文件类型必须为xls或xlsx，当前为: [%s]", extension));
			}
			LOG.info("校验成功！");
		}

		@Override
		public void prepare() {
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {

		}

		// 默认只有一个task读取excel
		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.info("begin do split...");
			List<Configuration> taskConfigList = new ArrayList<Configuration>();
			Configuration taskConfig = this.jobConfig.clone();
			taskConfigList.add(taskConfig);
			LOG.info("end do split...");
			return taskConfigList;
		}

	}

	public static class Task extends Reader.Task {
		private static final Logger LOG = LoggerFactory.getLogger(Task.class);

		private Configuration taskConfig;
		private List<String> cols;
		private String path;
		private List<DataSheet> dataSheets;

		@Override
		public void init() {
			this.taskConfig = this.getPluginJobConf();
			this.path = this.taskConfig.getString(Key.PATH);
			this.cols = this.taskConfig.getList(Key.COLUMN, String.class);
			@SuppressWarnings("rawtypes")
			List list = this.taskConfig.getList(Key.DATA_SHEETS, Map.class);
			this.dataSheets = this.Map2DataSheet(list);
		}

		@Override
		public void prepare() {

		}

		@Override
		public void startRead(RecordSender recordSender) {
			 LOG.info("begin do read...");
			try {
				List<Map<String, String>> rawDataList = ExcelUtil.parse2List(this.path, this.cols, this.dataSheets);
				for (Map<String, String> rawData : rawDataList) {
					Record record = generateRecord(rawData, recordSender);
					recordSender.sendToWriter(record);
				}
			} catch (FileNotFoundException e) {
				String message = String.format("找不到待读取的文件 : [%s]", this.path);
				LOG.error(message);
				throw DataXException.asDataXException(ExcelReaderErrorCode.FILE_NOT_EXISTS, message);
			} catch (IOException e) {
				String message = String.format("读取文件时出现异常 : [%s]", this.path);
				LOG.error(message);
				throw DataXException.asDataXException(ExcelReaderErrorCode.READ_FILE_IO_ERROR, message);
			} catch (Exception e) {
				e.printStackTrace();
				throw DataXException.asDataXException(ExcelReaderErrorCode.RECORD_SEND_ERROR, "生成record时出现异常！");
			}finally {
				recordSender.terminate();
			}
			 LOG.info("end do read...");
		}

		private Record generateRecord(Map<String, String> rawData, RecordSender recordSender) {
			Record record = recordSender.createRecord();
			for (String colName : this.cols) {
				String value = rawData.get(colName);
				Column column = new StringColumn(value);
				record.addColumn(column);
			}
			return record;
		}

		private List<DataSheet> Map2DataSheet(List<Map<String, Integer>> list) {
			List<DataSheet> result = new ArrayList<DataSheet>();
			if(list!=null && list.size()>0) {
				for (Map<String, Integer> map : list) {
					int skipRowNum = map.get(Key.SKIP_ROW_NUM);
					int sheetIndex = map.get(Key.SHEET_INDEX);
					DataSheet dataSheet = new DataSheet(sheetIndex, skipRowNum);
					result.add(dataSheet);
				}				
			}else {
				result.add(new DataSheet());
			}
			return result;
		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {

		}

	}
}
