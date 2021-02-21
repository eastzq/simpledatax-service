package com.github.simpledatax.api;

import com.github.simpledatax.api.adaptor.ConfigHelper;
import com.github.simpledatax.api.adaptor.exception.DxException;
import com.github.simpledatax.api.adaptor.util.PropertiesKey;
import com.github.simpledatax.api.adaptor.util.TimeLogHelper;
import com.github.simpledatax.api.dto.DataCollectJob;
import com.github.simpledatax.api.dto.DataCollectResult;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.core.Engine;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DxService {
    private static final Logger LOG = LoggerFactory.getLogger(DxService.class);

    public DataCollectResult collect(DataCollectJob job) throws DxException {
        DataCollectResult result = null;
        String key = (String) MDC.get(PropertiesKey.ACCEPT_ID);
        try {
            long jobId = job.getJobId();
            Configuration configuration = ConfigHelper.parseJob(job);

            if (StringUtils.isNotBlank(key)) {
                String wrapKey = key + "_job-" + jobId;
                MDC.put(PropertiesKey.ACCEPT_ID, wrapKey);
            } else {
                MDC.put(PropertiesKey.ACCEPT_ID, "job-" + jobId);
            }
            result = Engine.run(jobId, configuration);
        } catch (Throwable e) {
            LOG.error("采集处理异常！{}", e.getMessage());
            throw new DxException("采集处理出现异常，原因：" + e.getMessage(), e);
        } finally {
            if (StringUtils.isNotBlank(key)) {
                MDC.put(PropertiesKey.ACCEPT_ID, key);
            } else {
                MDC.remove(PropertiesKey.ACCEPT_ID);
            }
            TimeLogHelper.logAll();
        }
        return result;
    }
}
