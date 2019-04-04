package com.zq.simpledatax.core.taskgroup.runner;

import java.util.concurrent.Callable;

import com.zq.simpledatax.common.exception.DataXException;
import com.zq.simpledatax.core.taskgroup.TaskGroupContainer;
import com.zq.simpledatax.core.util.FrameworkErrorCode;
import com.zq.simpledatax.dataxservice.face.domain.enums.State;

public class TaskGroupContainerRunner implements Callable<Boolean> {

	private TaskGroupContainer taskGroupContainer;

	private State state;

	public TaskGroupContainerRunner(TaskGroupContainer taskGroup) {
		this.taskGroupContainer = taskGroup;
		this.state = State.SUCCEEDED;
	}
	

	@Override
	public Boolean call() throws Exception {
		try {
            Thread.currentThread().setName(
                    String.format("taskGroup-%d", this.taskGroupContainer.getTaskGroupId()));
            this.taskGroupContainer.start();
			this.state = State.SUCCEEDED;
		} catch (Throwable e) {
			this.state = State.FAILED;
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		}
		return this.state == State.SUCCEEDED;
	}

	public TaskGroupContainer getTaskGroupContainer() {
		return taskGroupContainer;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}
}
