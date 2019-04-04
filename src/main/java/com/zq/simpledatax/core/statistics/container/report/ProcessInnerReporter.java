package com.zq.simpledatax.core.statistics.container.report;

import com.zq.simpledatax.core.statistics.communication.Communication;

public class ProcessInnerReporter extends AbstractReporter {

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {
        // do nothing
    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {

    }
}