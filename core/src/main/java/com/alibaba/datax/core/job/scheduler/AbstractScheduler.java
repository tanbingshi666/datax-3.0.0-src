package com.alibaba.datax.core.job.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractScheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractScheduler.class);

    private ErrorRecordChecker errorLimit;

    private AbstractContainerCommunicator containerCommunicator;

    private Long jobId;

    public Long getJobId() {
        return jobId;
    }

    public AbstractScheduler(AbstractContainerCommunicator containerCommunicator) {
        // containerCommunicator = StandAloneJobContainerCommunicator
        // JobContainer 的 containerCommunicator 也拥有 containerCommunicator = StandAloneJobContainerCommunicator
        // JobContainer & StandaloneScheduler$AbstractScheduler 的 containerCommunicator 石同一个对象 StandAloneJobContainerCommunicator
        this.containerCommunicator = containerCommunicator;
    }

    /**
     * {
     * "common": {
     * "column": {
     * "dateFormat": "yyyy-MM-dd",
     * "datetimeFormat": "yyyy-MM-dd HH:mm:ss",
     * "encoding": "utf-8",
     * "extraFormats": ["yyyyMMdd"],
     * "timeFormat": "HH:mm:ss",
     * "timeZone": "GMT+8"
     * }* 	},
     * "core": {
     * "container": {
     * "job": {
     * "id": 1,
     * "mode": "standalone",
     * "reportInterval": 10000
     * }            ,
     * "taskGroup": {
     * "channel": 5,
     * "id": 0
     * },
     * "trace": {
     * "enable": "false"
     * }
     * },
     * "dataXServer": {
     * "address": "http://localhost:7001/api",
     * "reportDataxLog": false,
     * "reportPerfLog": false,
     * "timeout": 10000
     * },
     * "statistics": {
     * "collector": {
     * "plugin": {
     * "maxDirtyNumber": 10,
     * "taskClass": "com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector"
     * }
     * }
     * },
     * "transport": {
     * "channel": {
     * "byteCapacity": 67108864,
     * "capacity": 512,
     * "class": "com.alibaba.datax.core.transport.channel.memory.MemoryChannel",
     * "flowControlInterval": 20,
     * "speed": {
     * "byte": -1,
     * "record": -1
     * }
     * },
     * "exchanger": {
     * "bufferSize": 32,
     * "class": "com.alibaba.datax.core.plugin.BufferedRecordExchanger"
     * }
     * }* 	},
     * "entry": {
     * "jvm": "-Xms1G -Xmx1G"
     * }    ,
     * "job": {
     * "content": [{
     * "reader": {
     * "name": "streamreader",
     * "parameter": {
     * "column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
     * "sliceRecordCount": 10
     * }
     * },
     * "taskId": 4,
     * "writer": {
     * "name": "streamwriter",
     * "parameter": {
     * "encoding": "UTF-8",
     * "print": true
     * }
     * }
     * }, {
     * "reader": {
     * "name": "streamreader",
     * "parameter": {
     * "column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
     * "sliceRecordCount": 10
     * }
     * },
     * "taskId": 1,
     * "writer": {
     * "name": "streamwriter",
     * "parameter": {
     * "encoding": "UTF-8",
     * "print": true
     * }
     * }
     * }, {
     * "reader": {
     * "name": "streamreader",
     * "parameter": {
     * "column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
     * "sliceRecordCount": 10
     * }
     * },
     * "taskId": 3,
     * "writer": {
     * "name": "streamwriter",
     * "parameter": {
     * "encoding": "UTF-8",
     * "print": true
     * }
     * }
     * }, {
     * "reader": {
     * "name": "streamreader",
     * "parameter": {
     * "column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
     * "sliceRecordCount": 10
     * }
     * },
     * "taskId": 2,
     * "writer": {
     * "name": "streamwriter",
     * "parameter": {
     * "encoding": "UTF-8",
     * "print": true
     * }
     * }
     * }, {
     * "reader": {
     * "name": "streamreader",
     * "parameter": {
     * "column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
     * "sliceRecordCount": 10
     * }
     * },
     * "taskId": 0,
     * "writer": {
     * "name": "streamwriter",
     * "parameter": {
     * "encoding": "UTF-8",
     * "print": true
     * }
     * }
     * }],
     * "setting": {
     * "speed": {
     * "channel": 5
     * }
     * }
     * },
     * "plugin": {
     * "reader": {
     * "streamreader": {
     * "class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
     * "description": {
     * "mechanism": "use datax framework to transport data from stream.",
     * "useScene": "only for developer test.",
     * "warn": "Never use it in your real job."
     * },
     * "developer": "alibaba",
     * "name": "streamreader",
     * "path": "D:\\app\\datax\\plugin\\reader\\streamreader"
     * }
     * },
     * "writer": {
     * "streamwriter": {
     * "class": "com.alibaba.datax.plugin.writer.streamwriter.StreamWriter",
     * "description": {
     * "mechanism": "use datax framework to transport data to stream.",
     * "useScene": "only for developer test.",
     * "warn": "Never use it in your real job."
     * },
     * "developer": "alibaba",
     * "name": "streamwriter",
     * "path": "D:\\app\\datax\\plugin\\writer\\streamwriter"
     * }
     * }
     * }
     * }
     */
    public void schedule(List<Configuration> configurations) {
        Validate.notNull(configurations,
                "scheduler配置不能为空");

        // 获取任务上报间隔 默认 30s
        int jobReportIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 30000);

        // 获取任务睡眠间隔 默认 10s
        int jobSleepIntervalInMillSec = configurations.get(0).getInt(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL, 10000);

        // 获取 JobId
        this.jobId = configurations.get(0).getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);

        // 设置脏数据限制
        errorLimit = new ErrorRecordChecker(configurations.get(0));

        /**
         * 给 taskGroupContainer 的 Communication 注册
         * 意思是给每一个 TaskGroup 往 LocalTGCommunicationManager 注册对应的 Communication
         * 当前 job 对应一个 Communication 也即 0 -> Communication
         */
        // containerCommunicator = StandAloneJobContainerCommunicator
        this.containerCommunicator.registerCommunication(configurations);

        // 计算总任务数 基于场景驱动 为 5
        int totalTasks = calculateTaskCount(configurations);

        // 启动全部的 TaskGroup 调用 ProcessInnerScheduler.startAllTaskGroup()
        // 真正执行任务
        startAllTaskGroup(configurations);

        Communication lastJobContainerCommunication = new Communication();

        long lastReportTimeStamp = System.currentTimeMillis();
        try {
            // 默认每隔 30s 统计上报所有 TaskGroup 的统计信息
            while (true) {
                /**
                 * step 1: collect job stat
                 * step 2: getReport info, then report it
                 * step 3: errorLimit do check
                 * step 4: dealSucceedStat();
                 * step 5: dealKillingStat();
                 * step 6: dealFailedStat();
                 * step 7: refresh last job stat, and then sleep for next while
                 *
                 * above steps, some ones should report info to DS
                 *
                 */
                // containerCommunicator = StandAloneJobContainerCommunicator
                // 获取所有 TaskGroup 的 Communication 进行合并成一个 nowJobContainerCommunication = Communication
                Communication nowJobContainerCommunication = this.containerCommunicator.collect();
                nowJobContainerCommunication.setTimestamp(System.currentTimeMillis());
                LOG.debug(nowJobContainerCommunication.toString());

                //汇报周期
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > jobReportIntervalInMillSec) {
                    Communication reportCommunication = CommunicationTool
                            .getReportCommunication(nowJobContainerCommunication,
                                    lastJobContainerCommunication,
                                    totalTasks);

                    // 上报 JOB 的数据监控并打印
                    this.containerCommunicator.report(reportCommunication);
                    lastReportTimeStamp = now;
                    lastJobContainerCommunication = nowJobContainerCommunication;
                }

                errorLimit.checkRecordLimit(nowJobContainerCommunication);

                if (nowJobContainerCommunication.getState() == State.SUCCEEDED) {
                    LOG.info("Scheduler accomplished all tasks.");
                    break;
                }

                if (isJobKilling(this.getJobId())) {
                    dealKillingStat(this.containerCommunicator, totalTasks);
                } else if (nowJobContainerCommunication.getState() == State.FAILED) {
                    dealFailedStat(this.containerCommunicator, nowJobContainerCommunication.getThrowable());
                }

                Thread.sleep(jobSleepIntervalInMillSec);
            }
        } catch (InterruptedException e) {
            // 以 failed 状态退出
            LOG.error("捕获到InterruptedException异常!", e);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }

    }

    protected abstract void startAllTaskGroup(List<Configuration> configurations);

    protected abstract void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable);

    protected abstract void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks);

    private int calculateTaskCount(List<Configuration> configurations) {
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : configurations) {
            totalTasks += taskGroupConfiguration.getListConfiguration(
                    CoreConstant.DATAX_JOB_CONTENT).size();
        }
        return totalTasks;
    }

//    private boolean isJobKilling(Long jobId) {
//        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
//        return jobInfo.getData() == State.KILLING.value();
//    }

    protected abstract boolean isJobKilling(Long jobId);
}
