package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.core.statistics.communication.Communication;

public class StandaloneTGContainerCommunicator extends AbstractTGContainerCommunicator {

    /**
     *  创建 StandaloneTGContainerCommunicator
     */
    public StandaloneTGContainerCommunicator(Configuration configuration) {
        // 调用父类
        super(configuration);
        // 设置 Reporter
        super.setReporter(
                // 创建 ProcessInnerReporter
                new ProcessInnerReporter());
    }

    @Override
    public void report(Communication communication) {
        super.getReporter().reportTGCommunication(super.taskGroupId, communication);
    }

}
