package com.github.simpledatax.core;

import org.apache.commons.lang.Validate;

import com.github.simpledatax.common.util.Configuration;

/**
 * 执行容器的抽象类，持有该容器全局的配置 configuration
 */
public abstract class AbstractContainer {
    protected Configuration configuration;

    public AbstractContainer(Configuration configuration) {
        Validate.notNull(configuration, "Configuration can not be null.");

        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public abstract void start();

}
