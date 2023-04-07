package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ProcessInnerScheduler extends AbstractScheduler {

    private ExecutorService taskGroupContainerExecutorService;

    public ProcessInnerScheduler(AbstractContainerCommunicator containerCommunicator) {
        // 调用父类
        super(containerCommunicator);
    }

    @Override
    public void startAllTaskGroup(List<Configuration> configurations) {
        // 创建线程池 默认根据 Task Group 大小创建线程池大小 基于场景驱动 创建大小为 1
        this.taskGroupContainerExecutorService = Executors
                .newFixedThreadPool(configurations.size());

        for (Configuration taskGroupConfiguration : configurations) {

            // 创建 TaskGroupContainerRunner 是一个 Runnable 接口实现类
            // 因此核心处理业务逻辑在 run()
            TaskGroupContainerRunner taskGroupContainerRunner = newTaskGroupContainerRunner(taskGroupConfiguration);
            // 调用 TaskGroupContainerRunner run()
            this.taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
        }

        // 执行完 shutdown
        this.taskGroupContainerExecutorService.shutdown();
    }

    @Override
    public void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(
                FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }


    @Override
    public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        //通过进程退出返回码标示状态
        this.taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.asDataXException(FrameworkErrorCode.KILLED_EXIT_VALUE,
                "job killed status");
    }


    private TaskGroupContainerRunner newTaskGroupContainerRunner(
            Configuration configuration) {
        // 创建 TaskGroupContainer
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(configuration);

        // 创建 TaskGroupContainerRunner 是一个 Runnable 接口实现类
        // 因此核心处理业务逻辑在 run()
        return new TaskGroupContainerRunner(taskGroupContainer);
    }

}
