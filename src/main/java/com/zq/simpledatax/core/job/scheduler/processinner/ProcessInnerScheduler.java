package com.zq.simpledatax.core.job.scheduler.processinner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.zq.simpledatax.common.exception.DataXException;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.core.job.scheduler.AbstractScheduler;
import com.zq.simpledatax.core.statistics.communication.Communication;
import com.zq.simpledatax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.zq.simpledatax.core.taskgroup.TaskGroupContainer;
import com.zq.simpledatax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.zq.simpledatax.core.util.FrameworkErrorCode;

public abstract class ProcessInnerScheduler extends AbstractScheduler {

    private ExecutorService taskGroupContainerExecutorService;
    private List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
    
    public ProcessInnerScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    public void startAllTaskGroup(List<Configuration> configurations) {
        this.taskGroupContainerExecutorService = Executors
                .newFixedThreadPool(configurations.size());

        for (Configuration taskGroupConfiguration : configurations) {
            TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
            Future<Boolean> future = this.taskGroupContainerExecutorService.submit(taskGroupContainerRunner);
            futures.add(future);
        }
        this.taskGroupContainerExecutorService.shutdown();
    }

    @Override
    public void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(
                FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }
    
    @Override
    public List<Future<Boolean>> getFutures() {
		return futures;
	}

    @Override
    public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        //通过进程退出返回码标示状态
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                "job killed status");
    }

    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
    	Map<Integer, Communication> taskGroupCommunicationMap = super.getContainerCommunicator().getCollector().getTGCommunicationMap();
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(configuration,taskGroupCommunicationMap);
        return new TaskGroupContainerRunner(taskGroupContainer);  
    }
    
}
