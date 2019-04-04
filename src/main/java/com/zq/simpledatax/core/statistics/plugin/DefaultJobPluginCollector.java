package com.zq.simpledatax.core.statistics.plugin;

import java.util.List;
import java.util.Map;

import com.zq.simpledatax.common.plugin.JobPluginCollector;
import com.zq.simpledatax.core.statistics.communication.Communication;
import com.zq.simpledatax.core.statistics.container.communicator.AbstractContainerCommunicator;

/**
 * Created by jingxing on 14-9-9.
 */
public final class DefaultJobPluginCollector implements JobPluginCollector {
    private AbstractContainerCommunicator jobCollector;

    public DefaultJobPluginCollector(AbstractContainerCommunicator containerCollector) {
        this.jobCollector = containerCollector;
    }

    @Override
    public Map<String, List<String>> getMessage() {
        Communication totalCommunication = this.jobCollector.collect();
        return totalCommunication.getMessage();
    }

    @Override
    public List<String> getMessage(String key) {
        Communication totalCommunication = this.jobCollector.collect();
        return totalCommunication.getMessage(key);
    }
}
