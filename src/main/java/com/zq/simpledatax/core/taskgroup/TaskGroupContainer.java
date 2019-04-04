package com.zq.simpledatax.core.taskgroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zq.simpledatax.common.constant.PluginType;
import com.zq.simpledatax.common.exception.DataXException;
import com.zq.simpledatax.common.plugin.RecordSender;
import com.zq.simpledatax.common.plugin.TaskPluginCollector;
import com.zq.simpledatax.common.statistics.PerfRecord;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.core.AbstractContainer;
import com.zq.simpledatax.core.statistics.communication.Communication;
import com.zq.simpledatax.core.statistics.communication.CommunicationTool;
import com.zq.simpledatax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.zq.simpledatax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.zq.simpledatax.core.taskgroup.runner.AbstractRunner;
import com.zq.simpledatax.core.taskgroup.runner.ReaderRunner;
import com.zq.simpledatax.core.taskgroup.runner.WriterRunner;
import com.zq.simpledatax.core.transport.channel.Channel;
import com.zq.simpledatax.core.transport.exchanger.BufferedRecordExchanger;
import com.zq.simpledatax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.zq.simpledatax.core.transport.transformer.TransformerExecution;
import com.zq.simpledatax.core.util.ClassUtil;
import com.zq.simpledatax.core.util.FrameworkErrorCode;
import com.zq.simpledatax.core.util.TransformerUtil;
import com.zq.simpledatax.core.util.container.CoreConstant;
import com.zq.simpledatax.core.util.container.LoadUtil;
import com.zq.simpledatax.dataxservice.face.domain.enums.State;
import com.zq.simpledatax.helper.TimeLogHelper;

public class TaskGroupContainer extends AbstractContainer {
	private static final Logger LOG = LoggerFactory.getLogger(TaskGroupContainer.class);

	/**
	 * 当前taskGroup所属jobId
	 */
	private long jobId;

	/**
	 * 当前taskGroupId
	 */
	private int taskGroupId;

	/**
	 * 使用的channel类
	 */
	private String channelClazz;

	/**
	 * task收集器使用的类
	 */
	private String taskCollectorClass;

	private TaskMonitor taskMonitor = TaskMonitor.getInstance();

	private List<TaskExecutor> runTasks = null; // 正在运行task
	private Map<Integer, Long> taskStartTimeMap = new HashMap<Integer, Long>();

	public TaskGroupContainer(Configuration configuration, Map<Integer, Communication> taskGroupCommunicationMap) {
		super(configuration);
		initCommunicator(configuration, taskGroupCommunicationMap);
		this.jobId = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
		this.taskGroupId = this.configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
		this.channelClazz = this.configuration.getString(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
		this.taskCollectorClass = this.configuration
				.getString(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
	}

	private void initCommunicator(Configuration configuration, Map<Integer, Communication> taskGroupCommunicationMap) {
		StandaloneTGContainerCommunicator s = new StandaloneTGContainerCommunicator(configuration);
		s.getCollector().setTGCommunicationMap(taskGroupCommunicationMap);
		super.setContainerCommunicator(s);
	}

	public long getJobId() {
		return jobId;
	}

	public int getTaskGroupId() {
		return taskGroupId;
	}

	@Override
	public void start() {
		TimeLogHelper.start("taskGroup");
		try {
			// 获取channel数目
			int channelNumber = this.configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
			List<Configuration> taskConfigs = this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
			if (LOG.isDebugEnabled()) {
				LOG.debug("taskGroup[{}]'s task configs[{}]", this.taskGroupId, JSON.toJSONString(taskConfigs));
			}
			int taskCountInThisTaskGroup = taskConfigs.size();
			LOG.info(String.format("taskGroupId=[%d] start [%d] channels for [%d] tasks.", this.taskGroupId,
					channelNumber, taskCountInThisTaskGroup));
			this.containerCommunicator.registerCommunication(taskConfigs);
			Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskConfigs); // taskId与task配置
			List<Configuration> taskQueue = buildRemainTasks(taskConfigs); // 待运行task列表
			runTasks = new ArrayList<TaskExecutor>(channelNumber); // 正在运行task
			Communication lastTaskGroupContainerCommunication = new Communication();
			// 有任务未执行，且正在运行的任务数小于最大通道限制，执行任务，并等待返回结果！失败会返回false。
			boolean isSuccess = checkAndWaitResult(taskQueue, runTasks, channelNumber);
			// 任务执行完毕，汇总任务执行结果。
			lastTaskGroupContainerCommunication = reportTaskGroupCommunication(lastTaskGroupContainerCommunication,
					taskCountInThisTaskGroup);
			if (isSuccess) {
				// 获取所有的任务信息
				Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();
				// 遍历所有的communicationMap，对执行完毕的任务进行处理
				for (Map.Entry<Integer, Communication> entry : communicationMap.entrySet()) {
					Integer taskId = entry.getKey();
					Communication taskCommunication = entry.getValue();
					if (taskCommunication.getState() == State.SUCCEEDED) {
						Long taskStartTime = taskStartTimeMap.get(taskId);
						if (taskStartTime != null) {
							Long usedTime = System.currentTimeMillis() - taskStartTime;
							LOG.info("taskGroup[{}] taskId[{}] is successed, used[{}]ms", this.taskGroupId, taskId,
									usedTime);
							// usedTime*1000*1000 转换成PerfRecord记录的ns，这里主要是简单登记，进行最长任务的打印。因此增加特定静态方法
							PerfRecord.addPerfRecord(taskGroupId, taskId, PerfRecord.PHASE.TASK_TOTAL, taskStartTime,
									usedTime * 1000L * 1000L);
						}
					}
				}
				LOG.info("taskGroup[{}] completed it's tasks.", this.taskGroupId);
			} else {
				// 说明有任务执行失败了，停止所有尚未执行完毕的任务。并且抛出异常
				shutdownTasks();
				throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
						"任务执行失败，",lastTaskGroupContainerCommunication.getThrowable());
			}
		} catch (Throwable e) {
			Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
			if (nowTaskGroupContainerCommunication.getThrowable() == null) {
				nowTaskGroupContainerCommunication.setThrowable(e);
			}
			nowTaskGroupContainerCommunication.setState(State.FAILED);
			this.containerCommunicator.report(nowTaskGroupContainerCommunication);
			throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			// 清理引用，防止gc无法回收
			clearTaskMonitor(this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT));
			TimeLogHelper.end("taskGroup");
		}
	}

	private void shutdownTasks() {
		// 关闭未完成任务
		for (TaskExecutor task : runTasks) {
			if (!task.isShutdown()) {
				task.shutdown();
			}
		}
	}

	public void clearTaskMonitor(List<Configuration> configurations) {
		for (Configuration taskConfig : configurations) {
			int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
			taskMonitor.removeTask(taskId);
		}
	}

	// 执行任务并等待结果！
	private boolean checkAndWaitResult(List<Configuration> taskQueue, List<TaskExecutor> runTasks, int channelNumber) {
		// 有任务未执行，且正在运行的任务数小于最大通道限制，执行任务。
		while (true) {
			Iterator<Configuration> taskQueueIterator = taskQueue.iterator();
			while (taskQueueIterator.hasNext() && runTasks.size() < channelNumber) {
				Configuration taskConfig = taskQueueIterator.next();
				Integer taskId = taskConfig.getInt(CoreConstant.TASK_ID);
				int attemptCount = 0;
				TaskExecutor taskExecutor = new TaskExecutor(taskConfig, attemptCount);
				// 保存任务启动时间。
				taskStartTimeMap.put(taskId, System.currentTimeMillis());
				// 启动任务。
				taskExecutor.doStart();
				// 删除待执行任务
				taskQueueIterator.remove();
				// 保存到任务队列中。
				runTasks.add(taskExecutor);
				// 增加task到runTasks列表，因此在monitor里注册。
				taskMonitor.registerTask(taskId, this.containerCommunicator.getCommunication(taskId));
				LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] is started", this.taskGroupId, taskId,
						attemptCount);
			}
			Iterator<TaskExecutor> runTaskIterator = runTasks.iterator();
			while (runTaskIterator.hasNext()) {
				TaskExecutor task = runTaskIterator.next();
				boolean isTaskSuccess = false;
				boolean isTimeOut = false;
				try {
					isTaskSuccess = task.getTaskResult(400);
				} catch (TimeoutException e) {
					isTimeOut = true;
					if (LOG.isDebugEnabled()) {
						LOG.debug("请求任务结果超时---job[{}]，taskGroup[{}]，task[{}]，准备下一轮重新请求执行结果！", getJobId(),
								getTaskGroupId(), task.getTaskId());
					}
				}
				// 说明抛出了timeout异常。不做处理，执行下一轮
				if (isTimeOut) {
					continue;
				}
				if (isTaskSuccess) {
					runTaskIterator.remove();
				} else {
					// 执行失败返回false
					return false;
				}
			}
			// 说明有未完成的任务。重新遍历
			if (runTasks.size() == 0 && taskQueue.size() == 0) {
				break;
			}
		}
		return true;
	}

	private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> configurations) {
		Map<Integer, Configuration> map = new HashMap<Integer, Configuration>();
		for (Configuration taskConfig : configurations) {
			int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
			map.put(taskId, taskConfig);
		}
		return map;
	}

	private List<Configuration> buildRemainTasks(List<Configuration> configurations) {
		List<Configuration> remainTasks = new LinkedList<Configuration>();
		for (Configuration taskConfig : configurations) {
			remainTasks.add(taskConfig);
		}
		return remainTasks;
	}

	/**
	 * 删除从TaskExecutor列表里TaskExecutor
	 * 
	 * @param taskList
	 * @param taskId
	 * @return 当前移除的那个TaskExecutor
	 */
	private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId) {
		Iterator<TaskExecutor> iterator = taskList.iterator();
		while (iterator.hasNext()) {
			TaskExecutor taskExecutor = iterator.next();
			if (taskExecutor.getTaskId() == taskId) {
				iterator.remove();
				return taskExecutor;
			}
		}
		return null;
	}

	private boolean isAllTaskDone(List<TaskExecutor> taskList) {
		for (TaskExecutor taskExecutor : taskList) {
			if (!taskExecutor.isTaskFinished()) {
				return false;
			}
		}
		return true;
	}

	private Communication reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication,
			int taskCount) {
		Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
		nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
		Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
				lastTaskGroupContainerCommunication, taskCount);
		this.containerCommunicator.report(reportCommunication);
		return reportCommunication;
	}

	private void markCommunicationFailed(Integer taskId) {
		Communication communication = containerCommunicator.getCommunication(taskId);
		communication.setState(State.FAILED);
	}

	/**
	 * TaskExecutor是一个完整task的执行器 其中包括1：1的reader和writer
	 */
	class TaskExecutor {
		private Configuration taskConfig;

		private int taskId;

		private int attemptCount;

		private Channel channel;

		private Thread readerThread;

		private Thread writerThread;

		private FutureTask<Boolean> readerFutureTask;

		private FutureTask<Boolean> writerFutureTask;

		private ReaderRunner readerRunner;

		private WriterRunner writerRunner;

		/**
		 * 该处的taskCommunication在多处用到： 1. channel 2. readerRunner和writerRunner 3.
		 * reader和writer的taskPluginCollector
		 */
		private Communication taskCommunication;

		public TaskExecutor(Configuration taskConf, int attemptCount) {
			// 获取该taskExecutor的配置
			this.taskConfig = taskConf;
			Validate.isTrue(
					null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
							&& null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
					"[reader|writer]的插件参数不能为空!");

			// 得到taskId
			this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);
			this.attemptCount = attemptCount;

			/**
			 * 由taskId得到该taskExecutor的Communication
			 * 要传给readerRunner和writerRunner，同时要传给channel作统计用
			 */
			this.taskCommunication = containerCommunicator.getCommunication(taskId);
			Validate.notNull(this.taskCommunication, String.format("taskId[%d]的Communication没有注册过", taskId));
			this.channel = ClassUtil.instantiate(channelClazz, Channel.class, configuration);
			this.channel.setCommunication(this.taskCommunication);

			/**
			 * 获取transformer的参数
			 */

			List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(taskConfig);

			/**
			 * 生成writerThread
			 */

			writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
			this.writerFutureTask = new FutureTask<Boolean>(writerRunner);
			this.writerThread = new Thread(writerFutureTask,
					String.format("%d-%d-%d-writer", jobId, taskGroupId, this.taskId));


			/**
			 * 生成readerThread
			 */
			readerRunner = (ReaderRunner) generateRunner(PluginType.READER, transformerInfoExecs);
			this.readerFutureTask = new FutureTask<Boolean>(readerRunner);
			this.readerThread = new Thread(readerFutureTask,
					String.format("%d-%d-%d-reader", jobId, taskGroupId, this.taskId));

		}

		public void doStart() {
			this.writerThread.start();

			// reader没有起来，writer不可能结束
			if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
				throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
						this.taskCommunication.getThrowable());
			}

			this.readerThread.start();

			// 这里reader可能很快结束
			if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
				// 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
				throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
						this.taskCommunication.getThrowable());
			}

		}

		private AbstractRunner generateRunner(PluginType pluginType) {
			return generateRunner(pluginType, null);
		}

		private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs) {
			AbstractRunner newRunner = null;
			TaskPluginCollector pluginCollector;

			switch (pluginType) {
			case READER:
				newRunner = LoadUtil.loadPluginRunner(pluginType,
						this.taskConfig.getString(CoreConstant.JOB_READER_NAME));
				newRunner.setJobConf(this.taskConfig.getConfiguration(CoreConstant.JOB_READER_PARAMETER));

				pluginCollector = ClassUtil.instantiate(taskCollectorClass, AbstractTaskPluginCollector.class,
						configuration, this.taskCommunication, PluginType.READER);

				RecordSender recordSender;
				if (transformerInfoExecs != null && transformerInfoExecs.size() > 0) {
					recordSender = new BufferedRecordTransformerExchanger(taskGroupId, this.taskId, this.channel,
							this.taskCommunication, pluginCollector, transformerInfoExecs);
				} else {
					recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
				}

				((ReaderRunner) newRunner).setRecordSender(recordSender);

				/**
				 * 设置taskPlugin的collector，用来处理脏数据和job/task通信
				 */
				newRunner.setTaskPluginCollector(pluginCollector);
				break;
			case WRITER:
				newRunner = LoadUtil.loadPluginRunner(pluginType,
						this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME));
				newRunner.setJobConf(this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

				pluginCollector = ClassUtil.instantiate(taskCollectorClass, AbstractTaskPluginCollector.class,
						configuration, this.taskCommunication, PluginType.WRITER);
				((WriterRunner) newRunner)
						.setRecordReceiver(new BufferedRecordExchanger(this.channel, pluginCollector));
				/**
				 * 设置taskPlugin的collector，用来处理脏数据和job/task通信
				 */
				newRunner.setTaskPluginCollector(pluginCollector);
				break;
			default:
				throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR,
						"Cant generateRunner for:" + pluginType);
			}

			newRunner.setTaskGroupId(taskGroupId);
			newRunner.setTaskId(this.taskId);
			newRunner.setRunnerCommunication(this.taskCommunication);

			return newRunner;
		}

		// 检查任务是否结束
		private boolean isTaskFinished() {
			// 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
			if (readerThread.isAlive() || writerThread.isAlive()) {
				return false;
			}

			if (taskCommunication == null || !taskCommunication.isFinished()) {
				return false;
			}

			return true;
		}

		private int getTaskId() {
			return taskId;
		}

		private long getTimeStamp() {
			return taskCommunication.getTimestamp();
		}

		private int getAttemptCount() {
			return attemptCount;
		}

		private boolean supportFailOver() {
			return writerRunner.supportFailOver();
		}

		private void shutdown() {
			writerRunner.shutdown();
			readerRunner.shutdown();
			if (writerThread.isAlive()) {
				writerThread.interrupt();
			}
			if (readerThread.isAlive()) {
				readerThread.interrupt();
			}
		}

		private boolean isShutdown() {
			return !readerThread.isAlive() && !writerThread.isAlive();
		}

		private boolean getTaskResult(long timeout) throws TimeoutException {
			try {
				boolean isWriterSuccess = writerFutureTask.get(timeout / 2, TimeUnit.MILLISECONDS);
				boolean isReaderSuccess = readerFutureTask.get(timeout / 2, TimeUnit.MILLISECONDS);
				return isReaderSuccess && isWriterSuccess;
				// 异常已经在内部进行处理，此处不用做处理！
			} catch (InterruptedException e) {
				LOG.error("获取task结果被打断！");
				throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
			} catch (ExecutionException e) {
				LOG.error("task任务执行异常！"+e.getCause().toString());
				throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e.getCause());
			}
		}
	}
}
