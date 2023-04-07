package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.taskgroup.StandaloneTGContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.TransformerUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TaskGroupContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory
            .getLogger(TaskGroupContainer.class);

    /**
     * 当前taskGroup所属jobId
     */
    private long jobId;

    /**
     * 当前taskGroupId
     */
    private int taskGroupId;

    /**
     * 使用的channel类
     */
    private String channelClazz;

    /**
     * task收集器使用的类
     */
    private String taskCollectorClass;

    private TaskMonitor taskMonitor = TaskMonitor.getInstance();

    public TaskGroupContainer(Configuration configuration) {
        // 调用父类 本质还是赋值 Configuration
        super(configuration);

        // 初始化 StandaloneTGContainerCommunicator
        initCommunicator(configuration);

        // 获取 jobId core.container.job.id
        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);

        // 获取 taskGroupId core.container.taskGroup.id
        this.taskGroupId = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);

        // 获取 channelClazz core.transport.channel.class
        // 默认 com.alibaba.datax.core.transport.channel.memory.MemoryChannel
        this.channelClazz = this.configuration.getString(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);

        // 获取 taskCollectorClass core.statistics.collector.plugin.taskClass
        // taskCollectorClass = com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector
        this.taskCollectorClass = this.configuration.getString(
                CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    /**
     * 创建 StandaloneTGContainerCommunicator 并设置
     */
    private void initCommunicator(Configuration configuration) {
        super.setContainerCommunicator(
                // 创建 StandaloneTGContainerCommunicator
                new StandaloneTGContainerCommunicator(configuration));

    }

    public long getJobId() {
        return jobId;
    }

    public int getTaskGroupId() {
        return taskGroupId;
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
     * "taskId": 0,
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
    @Override
    public void start() {
        try {
            /**
             * 状态check时间间隔，较短，可以把任务及时分发到对应channel中
             */
            int sleepIntervalInMillSec = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 100);

            /**
             * 状态汇报时间间隔，稍长，避免大量汇报
             */
            long reportIntervalInMillSec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL,
                    10000);

            // 获取channel数目 默认情况下 一个 TaskGroup 对应 5 个 Channel
            int channelNumber = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);

            // 任务重试最大次数
            int taskMaxRetryTimes = this.configuration.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES, 1);

            // 任务重试间隔
            long taskRetryIntervalInMsec = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC, 10000);

            // 任务失败之后最大等待重试间隔
            long taskMaxWaitInMsec = this.configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC, 60000);

            // 获取任务配置信息 默认为 5
            /**
             * 5 个 Configuration 内容都是一致的 (taskId 不同)
             * {
             * 	"reader": {
             * 		"name": "streamreader",
             * 		"parameter": {
             * 			"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
             * 			"sliceRecordCount": 10
             *                }* 	},
             * 	"taskId": 4,
             * 	"writer": {
             * 		"name": "streamwriter",
             * 		"parameter": {
             * 			"encoding": "UTF-8",
             * 			"print": true
             *        }
             *    }
             * }
             */
            List<Configuration> taskConfigs = this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

            if (LOG.isDebugEnabled()) {
                LOG.debug("taskGroup[{}]'s task configs[{}]", this.taskGroupId,
                        JSON.toJSONString(taskConfigs));
            }

            // 当前 TaskGroup 的 task 个数 默认 5
            int taskCountInThisTaskGroup = taskConfigs.size();

            LOG.info(String.format(
                    "taskGroupId=[%d] start [%d] channels for [%d] tasks.",
                    this.taskGroupId, channelNumber, taskCountInThisTaskGroup));

            // 注册 也就保存 每个 TaskId 对应的 new Communication
            // containerCommunicator = StandaloneTGContainerCommunicator
            this.containerCommunicator.registerCommunication(taskConfigs);

            // 构建 TaskId -> Configuration
            Map<Integer, Configuration> taskConfigMap = buildTaskConfigMap(taskConfigs); //taskId与task配置

            // 将待运行配置添加到 taskQueue
            List<Configuration> taskQueue = buildRemainTasks(taskConfigs); //待运行task列表

            // 失败 TaskExecutor
            Map<Integer, TaskExecutor> taskFailedExecutorMap = new HashMap<Integer, TaskExecutor>(); //taskId与上次失败实例

            // 正在运行 TaskExecutor
            List<TaskExecutor> runTasks = new ArrayList<TaskExecutor>(channelNumber); //正在运行task

            // TaskId 对应的开始运行时间
            Map<Integer, Long> taskStartTimeMap = new HashMap<Integer, Long>(); //任务开始时间

            long lastReportTimeStamp = 0;
            // 创建 Communication
            Communication lastTaskGroupContainerCommunication = new Communication();

            while (true) {
                //1.判断task状态
                boolean failedOrKilled = false;
                // 获取 TaskId -> Communication
                // containerCommunicator = StandaloneTGContainerCommunicator
                Map<Integer, Communication> communicationMap = containerCommunicator.getCommunicationMap();

                // 判断当前 TaskGroup 的 每个 TaskId 对应的 TaskExecutor 是否完成
                // 以及容错处理
                for (Map.Entry<Integer, Communication> entry : communicationMap.entrySet()) {
                    // 遍历每个 TaskId
                    Integer taskId = entry.getKey();
                    // 获取 TaskId 对应的 Communication
                    Communication taskCommunication = entry.getValue();

                    // 基于场景驱动一开始是没有完成 所以第一次进来 后续代码是不会执行的
                    // 如果能进行往下执行 那么说明这个 TaskId 对应的 Task 已经完成，可以进行完成后续工作
                    if (!taskCommunication.isFinished()) {
                        continue;
                    }

                    // 从正在运行 Task 集合中根据 TaskId 移除返回正在运行的 TaskExecutor
                    // 第一次进来 runTasks 集合是没有 TaskExecutor 也即 taskExecutor = null
                    TaskExecutor taskExecutor = removeTask(runTasks, taskId);

                    // 上面从 runTasks 里移除了，因此对应在 monitor 里移除
                    taskMonitor.removeTask(taskId);

                    //失败，看task是否支持failover，重试次数未超过最大限制
                    if (taskCommunication.getState() == State.FAILED) {
                        taskFailedExecutorMap.put(taskId, taskExecutor);
                        if (taskExecutor.supportFailOver() && taskExecutor.getAttemptCount() < taskMaxRetryTimes) {
                            // 关闭老的executor
                            taskExecutor.shutdown();
                            // 将task的状态重置
                            containerCommunicator.resetCommunication(taskId);
                            Configuration taskConfig = taskConfigMap.get(taskId);
                            taskQueue.add(taskConfig); //重新加入任务列表
                        } else {
                            failedOrKilled = true;
                            break;
                        }
                    } else if (taskCommunication.getState() == State.KILLED) {
                        failedOrKilled = true;
                        break;
                    } else if (taskCommunication.getState() == State.SUCCEEDED) {
                        Long taskStartTime = taskStartTimeMap.get(taskId);
                        if (taskStartTime != null) {
                            Long usedTime = System.currentTimeMillis() - taskStartTime;
                            LOG.info("taskGroup[{}] taskId[{}] is successed, used[{}]ms",
                                    this.taskGroupId, taskId, usedTime);
                            //usedTime*1000*1000 转换成PerfRecord记录的ns，这里主要是简单登记，进行最长任务的打印。因此增加特定静态方法
                            PerfRecord.addPerfRecord(taskGroupId, taskId, PerfRecord.PHASE.TASK_TOTAL, taskStartTime, usedTime * 1000L * 1000L);
                            taskStartTimeMap.remove(taskId);
                            taskConfigMap.remove(taskId);
                        }
                    }
                }

                // 2.发现该taskGroup下taskExecutor的总状态失败则汇报错误
                if (failedOrKilled) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    throw DataXException.asDataXException(
                            FrameworkErrorCode.PLUGIN_RUNTIME_ERROR,
                            lastTaskGroupContainerCommunication.getThrowable());
                }

                //3.有任务未执行，且正在运行的任务数小于最大通道限制
                Iterator<Configuration> iterator = taskQueue.iterator();
                // 第一次进来开始执行封装任务
                while (iterator.hasNext() && runTasks.size() < channelNumber) {
                    // 这里开始真正执行任务
                    /**
                     * {
                     * 	"reader": {
                     * 		"name": "streamreader",
                     * 		"parameter": {
                     * 			"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
                     * 			"sliceRecordCount": 10
                     *                }* 	},
                     * 	"taskId": 4,
                     * 	"writer": {
                     * 		"name": "streamwriter",
                     * 		"parameter": {
                     * 			"encoding": "UTF-8",
                     * 			"print": true
                     *        }
                     *    }
                     * }
                     */
                    // taskConfig 标识执行任务配置
                    Configuration taskConfig = iterator.next();

                    // 获取 taskId
                    Integer taskId = taskConfig.getInt(CoreConstant.TASK_ID);

                    // 如果启动过程中失败了 尝试重启
                    int attemptCount = 1;

                    // 根据 taskId 获取失败的 TaskExecutor
                    TaskExecutor lastExecutor = taskFailedExecutorMap.get(taskId);

                    // 如果存在运行失败的任务
                    if (lastExecutor != null) {
                        attemptCount = lastExecutor.getAttemptCount() + 1;
                        long now = System.currentTimeMillis();
                        long failedTime = lastExecutor.getTimeStamp();
                        if (now - failedTime < taskRetryIntervalInMsec) {  //未到等待时间，继续留在队列
                            continue;
                        }
                        if (!lastExecutor.isShutdown()) { //上次失败的task仍未结束
                            if (now - failedTime > taskMaxWaitInMsec) {
                                markCommunicationFailed(taskId);
                                reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);
                                throw DataXException.asDataXException(CommonErrorCode.WAIT_TIME_EXCEED, "task failover等待超时");
                            } else {
                                lastExecutor.shutdown(); //再次尝试关闭
                                continue;
                            }
                        } else {
                            LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] has already shutdown",
                                    this.taskGroupId, taskId, lastExecutor.getAttemptCount());
                        }
                    }

                    // 检出待运行 Task Configuration
                    Configuration taskConfigForRun = taskMaxRetryTimes > 1 ? taskConfig.clone() : taskConfig;
                    // 创建 TaskExecutor
                    /**
                     * 根据 taskConfigForRun 配置文件创建对应的 Writer$Task(封装成 WriterRunner) Reader$Task (封装成 ReaderRunner)
                     * 并且创建 MemoryChannel 消息传递通道连接 ReaderRunner & WriterRunner
                     * 因为 ReaderRunner & WriterRunner 都是 Runnable 的实现类，所以都创建对应的 readerThread & writerThread
                     */
                    TaskExecutor taskExecutor = new TaskExecutor(taskConfigForRun, attemptCount);
                    // 添加 TaskId 对应的 Task 开始时间
                    taskStartTimeMap.put(taskId, System.currentTimeMillis());
                    // 真正执行任务 也即启动 readerThread & writerThread 对应的 ReaderRunner & WriterRunner 的 run()
                    taskExecutor.doStart();

                    // 移除待运行任务
                    iterator.remove();
                    // 添加正在运行任务
                    runTasks.add(taskExecutor);

                    //上面，增加task到runTasks列表，因此在monitor里注册。
                    taskMonitor.registerTask(taskId, this.containerCommunicator.getCommunication(taskId));

                    taskFailedExecutorMap.remove(taskId);
                    LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] is started",
                            this.taskGroupId, taskId, attemptCount);
                }

                //4.任务列表为空，executor已结束, 搜集状态为success--->成功
                if (taskQueue.isEmpty() && isAllTaskDone(runTasks) && containerCommunicator.collectState() == State.SUCCEEDED) {
                    // 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    LOG.info("taskGroup[{}] completed it's tasks.", this.taskGroupId);
                    break;
                }

                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

                    lastReportTimeStamp = now;

                    //taskMonitor对于正在运行的task，每reportIntervalInMillSec进行检查
                    for (TaskExecutor taskExecutor : runTasks) {
                        taskMonitor.report(taskExecutor.getTaskId(),
                                this.containerCommunicator.getCommunication(taskExecutor.getTaskId()));
                    }

                }

                Thread.sleep(sleepIntervalInMillSec);
            }

            //6.最后还要汇报一次
            reportTaskGroupCommunication(lastTaskGroupContainerCommunication, taskCountInThisTaskGroup);

        } catch (Throwable e) {
            Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();

            if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                nowTaskGroupContainerCommunication.setThrowable(e);
            }
            nowTaskGroupContainerCommunication.setState(State.FAILED);
            this.containerCommunicator.report(nowTaskGroupContainerCommunication);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        } finally {
            if (!PerfTrace.getInstance().isJob()) {
                //最后打印cpu的平均消耗，GC的统计
                VMInfo vmInfo = VMInfo.getVmInfo();
                if (vmInfo != null) {
                    vmInfo.getDelta(false);
                    LOG.info(vmInfo.totalString());
                }

                LOG.info(PerfTrace.getInstance().summarizeNoException());
            }
        }
    }

    private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> configurations) {
        Map<Integer, Configuration> map = new HashMap<Integer, Configuration>();
        for (Configuration taskConfig : configurations) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            map.put(taskId, taskConfig);
        }
        return map;
    }

    private List<Configuration> buildRemainTasks(List<Configuration> configurations) {
        List<Configuration> remainTasks = new LinkedList<Configuration>();
        for (Configuration taskConfig : configurations) {
            remainTasks.add(taskConfig);
        }
        return remainTasks;
    }

    private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId) {
        Iterator<TaskExecutor> iterator = taskList.iterator();
        while (iterator.hasNext()) {
            TaskExecutor taskExecutor = iterator.next();
            if (taskExecutor.getTaskId() == taskId) {
                iterator.remove();
                return taskExecutor;
            }
        }
        return null;
    }

    private boolean isAllTaskDone(List<TaskExecutor> taskList) {
        for (TaskExecutor taskExecutor : taskList) {
            if (!taskExecutor.isTaskFinished()) {
                return false;
            }
        }
        return true;
    }

    private Communication reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication, int taskCount) {
        // 收集 TaskGroup 的所有 TaskId 任务对应的监控数据
        Communication nowTaskGroupContainerCommunication = this.containerCommunicator.collect();
        nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());

        // 合并 Task Group下的所有 TaskId 对应的 TaskExecutor 对应的 Communication
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
                lastTaskGroupContainerCommunication, taskCount);

        // 上报 最终调用 LocalTGCommunicationManager
        this.containerCommunicator.report(reportCommunication);
        return reportCommunication;
    }

    private void markCommunicationFailed(Integer taskId) {
        Communication communication = containerCommunicator.getCommunication(taskId);
        communication.setState(State.FAILED);
    }

    /**
     * TaskExecutor是一个完整task的执行器
     * 其中包括1：1的reader和writer
     */
    class TaskExecutor {
        private Configuration taskConfig;

        private int taskId;

        private int attemptCount;

        private Channel channel;

        private Thread readerThread;

        private Thread writerThread;

        private ReaderRunner readerRunner;

        private WriterRunner writerRunner;

        /**
         * 该处的taskCommunication在多处用到：
         * 1. channel
         * 2. readerRunner和writerRunner
         * 3. reader和writer的taskPluginCollector
         */
        private Communication taskCommunication;

        public TaskExecutor(Configuration taskConf, int attemptCount) {
            // 获取该taskExecutor的配置
            /**
             * {
             * 	"reader": {
             * 		"name": "streamreader",
             * 		"parameter": {
             * 			"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
             * 			"sliceRecordCount": 10
             *                }* 	},
             * 	"taskId": 4,
             * 	"writer": {
             * 		"name": "streamwriter",
             * 		"parameter": {
             * 			"encoding": "UTF-8",
             * 			"print": true
             *        }
             *    }
             * }
             */
            this.taskConfig = taskConf;
            Validate.isTrue(null != this.taskConfig.getConfiguration(CoreConstant.JOB_READER)
                            && null != this.taskConfig.getConfiguration(CoreConstant.JOB_WRITER),
                    "[reader|writer]的插件参数不能为空!");

            // 得到taskId
            this.taskId = this.taskConfig.getInt(CoreConstant.TASK_ID);
            this.attemptCount = attemptCount;

            /**
             * 由 taskId 得到该 taskExecutor 的 Communication
             * 要传给 readerRunner和 writerRunner，同时要传给 channel 作统计用
             */
            this.taskCommunication = containerCommunicator
                    .getCommunication(taskId);
            Validate.notNull(this.taskCommunication,
                    String.format("taskId[%d]的Communication没有注册过", taskId));
            // channel 默认为 com.alibaba.datax.core.transport.channel.memory.MemoryChannel
            // 那么将会调用 MemoryChannel 的 入参为 Configuration 的构造函数
            this.channel = ClassUtil.instantiate(channelClazz,
                    Channel.class, configuration);
            // 意思是每个 TaskExecutor 根据 MemoryChannel 都绑定了同一个 Communication
            this.channel.setCommunication(this.taskCommunication);

            /**
             * 获取transformer的参数
             */
            // 默认为 null
            List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(taskConfig);

            /**
             * 生成 writerThread
             */
            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
            // 创建 WriterThread
            this.writerThread = new Thread(writerRunner,
                    String.format("%d-%d-%d-writer",
                            jobId, taskGroupId, this.taskId));
            //通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
            this.writerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.WRITER, this.taskConfig.getString(
                            CoreConstant.JOB_WRITER_NAME)));

            /**
             * 生成readerThread
             */
            readerRunner = (ReaderRunner) generateRunner(PluginType.READER, transformerInfoExecs);
            // 创建 ReaderThread
            this.readerThread = new Thread(readerRunner,
                    String.format("%d-%d-%d-reader",
                            jobId, taskGroupId, this.taskId));
            /**
             * 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
             */
            this.readerThread.setContextClassLoader(LoadUtil.getJarLoader(
                    PluginType.READER, this.taskConfig.getString(
                            CoreConstant.JOB_READER_NAME)));
        }

        public void doStart() {
            // 调用 WriterRunner 的 run()
            this.writerThread.start();

            // reader没有起来，writer不可能结束
            if (!this.writerThread.isAlive() || this.taskCommunication.getState() == State.FAILED) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

            // 调用 ReaderRunner run()
            this.readerThread.start();

            // 这里reader可能很快结束
            if (!this.readerThread.isAlive() && this.taskCommunication.getState() == State.FAILED) {
                // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        this.taskCommunication.getThrowable());
            }

        }


        private AbstractRunner generateRunner(PluginType pluginType) {
            // 根据 PluginType[reader|writer] 生成对应的 ReaderRunner|WriterRunner
            return generateRunner(pluginType, null);
        }

        private AbstractRunner generateRunner(PluginType pluginType, List<TransformerExecution> transformerInfoExecs) {
            AbstractRunner newRunner = null;
            TaskPluginCollector pluginCollector;

            switch (pluginType) {
                case READER:
                    // 创建 ReaderRunner 是一个 Runnable 接口的实现类
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_READER_NAME));
                    newRunner.setJobConf(this.taskConfig.getConfiguration(
                            CoreConstant.JOB_READER_PARAMETER));

                    // taskCollectorClass = StdoutPluginCollector
                    // 调用 StdoutPluginCollector(configuration,this.taskCommunication,PluginType.READER) 构造函数
                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass,
                            AbstractTaskPluginCollector.class,
                            configuration,
                            this.taskCommunication,
                            PluginType.READER);

                    // 将 Reader 生产的数据绑定到 Channel
                    RecordSender recordSender;
                    if (transformerInfoExecs != null && transformerInfoExecs.size() > 0) {
                        recordSender = new BufferedRecordTransformerExchanger(taskGroupId, this.taskId, this.channel, this.taskCommunication, pluginCollector, transformerInfoExecs);
                    } else {
                        // 创建 BufferedRecordExchanger
                        recordSender = new BufferedRecordExchanger(this.channel, pluginCollector);
                    }

                    // 设置 BufferedRecordExchanger
                    ((ReaderRunner) newRunner).setRecordSender(recordSender);

                    /**
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                case WRITER:
                    // 创建 WriterRunner 是一个 Runnable 接口的实现类
                    newRunner = LoadUtil.loadPluginRunner(pluginType,
                            this.taskConfig.getString(CoreConstant.JOB_WRITER_NAME));
                    newRunner.setJobConf(this.taskConfig
                            .getConfiguration(CoreConstant.JOB_WRITER_PARAMETER));

                    // taskCollectorClass = StdoutPluginCollector
                    // 调用 StdoutPluginCollector(configuration,this.taskCommunication,PluginType.READER) 构造函数
                    pluginCollector = ClassUtil.instantiate(
                            taskCollectorClass,
                            AbstractTaskPluginCollector.class,
                            configuration,
                            this.taskCommunication,
                            PluginType.WRITER);
                    // 绑定 WriterRunner 与 Channel
                    ((WriterRunner) newRunner).setRecordReceiver(new BufferedRecordExchanger(
                            this.channel, pluginCollector));
                    /**
                     * 设置taskPlugin的collector，用来处理脏数据和job/task通信
                     */
                    newRunner.setTaskPluginCollector(pluginCollector);
                    break;
                default:
                    throw DataXException.asDataXException(FrameworkErrorCode.ARGUMENT_ERROR, "Cant generateRunner for:" + pluginType);
            }

            // 设置 TaskGroupId
            newRunner.setTaskGroupId(taskGroupId);
            // 设置 TaskId
            newRunner.setTaskId(this.taskId);
            // 设置 Communication
            newRunner.setRunnerCommunication(this.taskCommunication);

            return newRunner;
        }

        // 检查任务是否结束
        private boolean isTaskFinished() {
            // 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
            if (readerThread.isAlive() || writerThread.isAlive()) {
                return false;
            }

            if (taskCommunication == null || !taskCommunication.isFinished()) {
                return false;
            }

            return true;
        }

        private int getTaskId() {
            return taskId;
        }

        private long getTimeStamp() {
            return taskCommunication.getTimestamp();
        }

        private int getAttemptCount() {
            return attemptCount;
        }

        private boolean supportFailOver() {
            return writerRunner.supportFailOver();
        }

        private void shutdown() {
            writerRunner.shutdown();
            readerRunner.shutdown();
            if (writerThread.isAlive()) {
                writerThread.interrupt();
            }
            if (readerThread.isAlive()) {
                readerThread.interrupt();
            }
        }

        private boolean isShutdown() {
            return !readerThread.isAlive() && !writerThread.isAlive();
        }
    }
}
