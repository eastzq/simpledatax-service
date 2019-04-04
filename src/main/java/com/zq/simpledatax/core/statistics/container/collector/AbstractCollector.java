package com.zq.simpledatax.core.statistics.container.collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.Validate;

import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.core.statistics.communication.Communication;
import com.zq.simpledatax.core.util.container.CoreConstant;
import com.zq.simpledatax.dataxservice.face.domain.enums.State;

public abstract class AbstractCollector {
	private Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<Integer, Communication>();
	private Map<Integer, Communication> taskGroupCommunicationMap = new ConcurrentHashMap<Integer, Communication>();

	private Long jobId;

	public Map<Integer, Communication> getTaskCommunicationMap() {
		return taskCommunicationMap;
	}

	public Long getJobId() {
		return jobId;
	}

	public void setJobId(Long jobId) {
		this.jobId = jobId;
	}

	public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
		for (Configuration config : taskGroupConfigurationList) {
			int taskGroupId = config.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
			taskGroupCommunicationMap.put(taskGroupId, new Communication());
		}
	}

	public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
		for (Configuration taskConfig : taskConfigurationList) {
			int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
			this.taskCommunicationMap.put(taskId, new Communication());
		}
	}

	public Communication collectFromTask() {
		Communication communication = new Communication();
		communication.setState(State.SUCCEEDED);
		for (Communication taskCommunication : this.taskCommunicationMap.values()) {
			communication.mergeFrom(taskCommunication);
		}
		return communication;
	}

	public Communication collectFromTaskGroup() {
		Communication communication = new Communication();
		communication.setState(State.SUCCEEDED);
		for (Communication taskGroupCommunication : this.taskGroupCommunicationMap.values()) {
			communication.mergeFrom(taskGroupCommunication);
		}
		return communication;
	}

	public Map<Integer, Communication> getTGCommunicationMap() {
		return taskGroupCommunicationMap;
	}

	public Communication getTGCommunication(Integer taskGroupId) {
		Validate.isTrue(taskGroupId >= 0, "taskGroupId不能小于0");
		return taskGroupCommunicationMap.get(taskGroupId);
	}

	public Communication getTaskCommunication(Integer taskId) {
		return this.taskCommunicationMap.get(taskId);
	}

	public void updateTaskGroupCommunication(final int taskGroupId, final Communication communication) {
		Validate.isTrue(taskGroupCommunicationMap.containsKey(taskGroupId), String.format(
				"taskGroupCommunicationMap中没有注册taskGroupId[%d]的Communication，" + "无法更新该taskGroup的信息", taskGroupId));
		taskGroupCommunicationMap.put(taskGroupId, communication);
	}
	

	public void setTGCommunicationMap(Map<Integer, Communication> taskGroupCommunicationMap) {
		this.taskGroupCommunicationMap = taskGroupCommunicationMap;
	}

}
