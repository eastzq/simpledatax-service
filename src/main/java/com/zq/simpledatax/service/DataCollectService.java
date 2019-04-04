package com.zq.simpledatax.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zq.simpledatax.api.message.DataCollectJob;
import com.zq.simpledatax.api.message.DataCollectResult;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.core.Engine;
import com.zq.simpledatax.helper.ConfigHelper;
import com.zq.simpledatax.helper.PropertiesKey;
import com.zq.simpledatax.helper.TimeLogHelper;
import com.zq.simpledatax.helper.exception.BusiException;


public class DataCollectService {
	private static final Logger LOG = LoggerFactory.getLogger(DataCollectService.class);
	
	public DataCollectResult collect(DataCollectJob job) throws BusiException {
		DataCollectResult result = null;
		String key = (String) MDC.get(PropertiesKey.ACCEPT_ID);
		try {
			long jobId = job.getJobId();
			Configuration configuration = ConfigHelper.parseJob(job);
			
			if(StringUtils.isNotBlank(key)) {
				String wrapKey=key+"_job-"+jobId;
				MDC.put(PropertiesKey.ACCEPT_ID, wrapKey);
			}else {
				MDC.put(PropertiesKey.ACCEPT_ID,"job-"+jobId);
			}
			
			TimeLogHelper.start("engine");
			result = Engine.xy_entry(jobId, configuration);
			TimeLogHelper.end("engine");
		} catch (Throwable e) {
			LOG.error("采集处理异常！{}",e.getMessage());
			throw new BusiException("采集处理出现异常，原因：" + e.getMessage(),e);
		}finally {
			MDC.put(PropertiesKey.ACCEPT_ID, key);
			TimeLogHelper.logAll();
		}
		return result;
	}
}
