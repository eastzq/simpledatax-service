package com.zq.simpledatax.core.statistics.container.communicator.taskgroup;

import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.core.statistics.communication.Communication;
import com.zq.simpledatax.core.statistics.container.report.ProcessInnerReporter;

public class StandaloneTGContainerCommunicator extends AbstractTGContainerCommunicator {

    public StandaloneTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        super.setReporter(new ProcessInnerReporter());
    }

    @Override
    public void report(Communication communication) {
        super.getCollector().updateTaskGroupCommunication(super.taskGroupId, communication);
    }

}
