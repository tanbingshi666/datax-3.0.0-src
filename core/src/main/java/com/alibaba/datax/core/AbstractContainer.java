package com.alibaba.datax.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import org.apache.commons.lang.Validate;

/**
 * 执行容器的抽象类，持有该容器全局的配置 configuration
 */
public abstract class AbstractContainer {
    protected Configuration configuration;

    protected AbstractContainerCommunicator containerCommunicator;

    public AbstractContainer(Configuration configuration) {
        Validate.notNull(configuration, "Configuration can not be null.");

        // 设置 Configuration 表示当前运行的所有配置信息都在这个 Configuration
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public AbstractContainerCommunicator getContainerCommunicator() {
        return containerCommunicator;
    }

    public void setContainerCommunicator(AbstractContainerCommunicator containerCommunicator) {
        // 如果是 JobContainer 设置 containerCommunicator = StandAloneJobContainerCommunicator
        // 如果是 TaskGroupContainer 设置 containerCommunicator = StandaloneTGContainerCommunicator
        this.containerCommunicator = containerCommunicator;
    }

    /**
     * 调用 JobContainer 或者 TaskGroupContainer
     * 这里基于场景驱动调用 JobContainer
     */
    public abstract void start();

}
