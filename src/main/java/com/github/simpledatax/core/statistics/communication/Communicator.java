package com.github.simpledatax.core.statistics.communication;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.core.util.container.CoreConstant;
import com.github.simpledatax.dataxservice.face.domain.enums.State;

public class Communicator {

    private Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<Integer, Communication>();

    protected Long jobId;

    public Long getJobId() {
        return jobId;
    }

    public Communicator() {
    }

    public Map<Integer, Communication> getTaskCommunicationMap() {
        return taskCommunicationMap;
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

    public Communication getTaskCommunication(Integer taskId) {
        return this.taskCommunicationMap.get(taskId);
    }

    public void registerCommunication(List<Configuration> configurationList) {
        this.registerTaskCommunication(configurationList);
    }

    public Communication collect() {
        return this.collectFromTask();
    }

    public void report(Communication communication) {

    }

    public State collectState() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);
        for (Communication taskCommunication : this.getTaskCommunicationMap().values()) {
            communication.mergeStateFrom(taskCommunication);
        }
        return communication.getState();
    }

    public  Communication getCommunication(Integer id) {
        return this.taskCommunicationMap.get(id);
    }

    public  Map<Integer, Communication> getCommunicationMap(){
        return this.getTaskCommunicationMap();
    }

    public void resetCommunication(Integer id) {
        Map<Integer, Communication> map = getCommunicationMap();
        map.put(id, new Communication());
    }

    public void reportVmInfo() {
        /*
         * long now = System.currentTimeMillis(); //每5分钟打印一次 if(now - lastReportTime >=
         * 300000) { //当前仅打印 if (vmInfo != null) { vmInfo.getDelta(true); }
         * lastReportTime = now; }
         */
    }
}