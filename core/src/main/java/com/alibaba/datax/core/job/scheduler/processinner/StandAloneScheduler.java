package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;

/**
 * Created by hongjiao.hj on 2014/12/22.
 */
public class StandAloneScheduler extends ProcessInnerScheduler{

    /**
     * containerCommunicator = StandAloneJobContainerCommunicator
     * 基于场景驱动 JobContainer 的 也拥有 containerCommunicator = StandAloneJobContainerCommunicator
     * 两者 containerCommunicator 都是同一个对象
     */
    public StandAloneScheduler(AbstractContainerCommunicator containerCommunicator) {
        // 调用父类
        super(containerCommunicator);
    }

    @Override
    protected boolean isJobKilling(Long jobId) {
        return false;
    }

}
