package com.github.simpledatax.core.statistics.plugin.task;

import com.github.simpledatax.common.constant.PluginType;
import com.github.simpledatax.common.element.Record;
import com.github.simpledatax.common.util.Configuration;
import com.github.simpledatax.core.statistics.communication.Communication;

/**
 * Created by jingxing on 14-9-9.
 */
public class HttpPluginCollector extends AbstractTaskPluginCollector {
    public HttpPluginCollector(Configuration configuration, Communication Communication,
                               PluginType type) {
        super(configuration, Communication, type);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t,
                                   String errorMessage) {
        super.collectDirtyRecord(dirtyRecord, t, errorMessage);
    }

}
