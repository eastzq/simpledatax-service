package com.zq.simpledatax.core.job.scheduler;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zq.simpledatax.common.exception.DataXException;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.core.statistics.communication.Communication;
import com.zq.simpledatax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.zq.simpledatax.core.util.ErrorRecordChecker;
import com.zq.simpledatax.core.util.FrameworkErrorCode;
import com.zq.simpledatax.core.util.container.CoreConstant;
import com.zq.simpledatax.dataxservice.face.domain.enums.State;

/**
 * 进度表时刻表 调度
 */
public abstract class AbstractScheduler {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractScheduler.class);

	private ErrorRecordChecker errorLimit;

	private AbstractContainerCommunicator containerCommunicator;

	private Long jobId;

	public Long getJobId() {
		return jobId;
	}

	public AbstractScheduler(AbstractContainerCommunicator containerCommunicator) {
		this.containerCommunicator = containerCommunicator;
	}

	public void schedule(List<Configuration> configurations) {
		Validate.notNull(configurations, "scheduler配置不能为空");
		this.jobId = configurations.get(0).getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
		errorLimit = new ErrorRecordChecker(configurations.get(0));
		/**
		 * 给 taskGroupContainer 的 Communication 注册
		 */
		this.containerCommunicator.registerCommunication(configurations);
		startAllTaskGroup(configurations);
		
		boolean isSuccess = checkAndWait(this.getFutures());
		
		Communication nowJobContainerCommunication = this.containerCommunicator.collect();	
		nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
		errorLimit.checkRecordLimit(nowJobContainerCommunication);
		if(isSuccess && nowJobContainerCommunication.getState() == State.SUCCEEDED) {
			LOG.info("Scheduler accomplished all tasks.");
		}else if (nowJobContainerCommunication.getState() == State.FAILED) {
			dealFailedStat(this.containerCommunicator, nowJobContainerCommunication.getThrowable());
		}
	}
	public Boolean checkAndWait(List<Future<Boolean>> futures) {
		while (true) {
			boolean isTaskGroupSuccess = false;
			Iterator<Future<Boolean>> futureIterator = futures.iterator();
			while (futureIterator.hasNext()) {
				Future<Boolean> future = futureIterator.next();
				boolean isTimeOut = false;
				try {
					isTaskGroupSuccess = getTaskGroupResult(future);
				} catch (TimeoutException e) {
					isTimeOut = true;
					if (LOG.isDebugEnabled()) {
						LOG.debug("请求job结果超时---job[{}]准备下一轮重新请求执行结果！", getJobId());
					}
				}
				if (isTimeOut) {
					continue;
				}
				if (isTaskGroupSuccess) {
					futureIterator.remove();
				} else {
					// 执行失败返回false
					return false;
				}
			}
			if(futures.size()==0) {
				break;
			}
		}
		return true;
	}

	public boolean getTaskGroupResult(Future<Boolean> future) throws TimeoutException {
		Boolean st = false;
		try {
			st = future.get(2000, TimeUnit.MILLISECONDS);
		} catch (ExecutionException e) {
			LOG.error("捕获到TaskGroup内部异常", e.getCause().toString());
			throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e.getCause());
		} catch (InterruptedException e) {
			LOG.error("捕获到InterruptedException异常!", e.toString());
			throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
		}
		return st;
	}

	protected abstract void startAllTaskGroup(List<Configuration> configurations);

	protected abstract void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable);

	protected abstract void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks);

	/**
	 * 获取返回结果
	 * 
	 * @return
	 */
	protected abstract List<Future<Boolean>> getFutures();

	private int calculateTaskCount(List<Configuration> configurations) {
		int totalTasks = 0;
		for (Configuration taskGroupConfiguration : configurations) {
			totalTasks += taskGroupConfiguration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT).size();
		}
		return totalTasks;
	}

	// private boolean isJobKilling(Long jobId) {
	// Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
	// return jobInfo.getData() == State.KILLING.value();
	// }

	protected abstract boolean isJobKilling(Long jobId);

	public AbstractContainerCommunicator getContainerCommunicator() {
		return containerCommunicator;
	}

	public void setContainerCommunicator(AbstractContainerCommunicator containerCommunicator) {
		this.containerCommunicator = containerCommunicator;
	}
}
