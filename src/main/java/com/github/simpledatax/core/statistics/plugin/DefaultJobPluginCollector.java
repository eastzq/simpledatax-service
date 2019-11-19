package com.github.simpledatax.core.statistics.plugin;

import java.util.List;
import java.util.Map;

import com.github.simpledatax.common.plugin.JobPluginCollector;
import com.github.simpledatax.core.statistics.communication.Communication;
import com.github.simpledatax.core.statistics.communication.Communicator;

/**
 * Created by jingxing on 14-9-9.
 */
public final class DefaultJobPluginCollector implements JobPluginCollector {

    @Override
    public Map<String, List<String>> getMessage() {
        return null;
    }

    @Override
    public List<String> getMessage(String key) {
        return null;
    }

}
