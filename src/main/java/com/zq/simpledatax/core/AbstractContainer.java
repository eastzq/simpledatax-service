package com.zq.simpledatax.core;

import org.apache.commons.lang.Validate;

import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.core.statistics.container.communicator.AbstractContainerCommunicator;

/**
 * 执行容器的抽象类，持有该容器全局的配置 configuration
 */
public abstract class AbstractContainer {
    protected Configuration configuration;

    protected AbstractContainerCommunicator containerCommunicator;

    public AbstractContainer(Configuration configuration) {
        Validate.notNull(configuration, "Configuration can not be null.");

        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public AbstractContainerCommunicator getContainerCommunicator() {
        return containerCommunicator;
    }

    public void setContainerCommunicator(AbstractContainerCommunicator containerCommunicator) {
        this.containerCommunicator = containerCommunicator;
    }

    public abstract void start();

}
