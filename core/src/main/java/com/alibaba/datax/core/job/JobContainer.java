package com.alibaba.datax.core.job;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.container.util.HookInvoker;
import com.alibaba.datax.core.container.util.JobAssignUtil;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.job.scheduler.processinner.StandAloneScheduler;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.DefaultJobPluginCollector;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * job实例运行在jobContainer容器中，它是所有任务的master，负责初始化、拆分、调度、运行、回收、监控和汇报
 * 但它并不做实际的数据同步操作
 */
public class JobContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory
            .getLogger(JobContainer.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");

    private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
            .newCurrentThreadClassLoaderSwapper();

    private long jobId;

    private String readerPluginName;

    private String writerPluginName;

    /**
     * reader和writer jobContainer的实例
     */
    // init() -> StreamReader$Job
    private Reader.Job jobReader;
    // init() -> StreamWriter$Job
    private Writer.Job jobWriter;

    private Configuration userConf;

    private long startTimeStamp;

    private long endTimeStamp;

    private long startTransferTimeStamp;

    private long endTransferTimeStamp;

    // 默认为 5
    private int needChannelNumber;

    private int totalStage = 1;

    private ErrorRecordChecker errorLimit;

    public JobContainer(Configuration configuration) {
        // 赋值 Configuration
        super(configuration);

        // 设置 脏数据条数
        errorLimit = new ErrorRecordChecker(configuration);
    }

    /**
     * jobContainer主要负责的工作全部在 start() 里面，包括init、prepare、split、scheduler、
     * post以及 destroy 和 statistics
     */
    @Override
    public void start() {
        LOG.info("DataX jobContainer starts job.");

        boolean hasException = false;
        boolean isDryRun = false;
        try {
            // 获取当前时间戳
            this.startTimeStamp = System.currentTimeMillis();
            // 默认为 false
            isDryRun = configuration.getBool(CoreConstant.DATAX_JOB_SETTING_DRYRUN, false);

            if (isDryRun) {
                LOG.info("jobContainer starts to do preCheck ...");
                this.preCheck();
            } else {
                // 拷贝当前任务涉及到的全部配置文件 (深拷贝)
                userConf = configuration.clone();

                LOG.debug("jobContainer starts to do preHandle ...");
                // 前置处理器
                this.preHandle();

                LOG.debug("jobContainer starts to do init ...");
                // 初始化 主要初始化 StreamReader$Job StreamWriter$Job 并解析参数调用 init()
                this.init();

                LOG.info("jobContainer starts to do prepare ...");
                // 准备环境 (调用 StreamReader$Job StreamWriter$Job prepare())
                this.prepare();

                LOG.info("jobContainer starts to do split ...");
                // 切分任务 (任务切分完成并且将切分好的配置信息( reader & writer) 重新覆盖 configuration 的 job.content 内容)
                this.totalStage = this.split();

                LOG.info("jobContainer starts to do schedule ...");
                // 调度执行
                this.schedule();

                LOG.debug("jobContainer starts to do post ...");
                this.post();

                LOG.debug("jobContainer starts to do postHandle ...");
                // 后置处理器
                this.postHandle();
                LOG.info("DataX jobId [{}] completed successfully.", this.jobId);

                // 触发钩子
                this.invokeHooks();
            }
        } catch (Throwable e) {
            LOG.error("Exception when job run", e);

            hasException = true;

            if (e instanceof OutOfMemoryError) {
                this.destroy();
                System.gc();
            }


            if (super.getContainerCommunicator() == null) {
                // 由于 containerCollector 是在 scheduler() 中初始化的，所以当在 scheduler() 之前出现异常时，需要在此处对 containerCollector 进行初始化

                AbstractContainerCommunicator tempContainerCollector;
                // standalone
                tempContainerCollector = new StandAloneJobContainerCommunicator(configuration);

                super.setContainerCommunicator(tempContainerCollector);
            }

            Communication communication = super.getContainerCommunicator().collect();
            // 汇报前的状态，不需要手动进行设置
            // communication.setState(State.FAILED);
            communication.setThrowable(e);
            communication.setTimestamp(this.endTimeStamp);

            Communication tempComm = new Communication();
            tempComm.setTimestamp(this.startTransferTimeStamp);

            Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);
            super.getContainerCommunicator().report(reportCommunication);

            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        } finally {
            if (!isDryRun) {

                this.destroy();
                this.endTimeStamp = System.currentTimeMillis();
                if (!hasException) {
                    //最后打印cpu的平均消耗，GC的统计
                    VMInfo vmInfo = VMInfo.getVmInfo();
                    if (vmInfo != null) {
                        vmInfo.getDelta(false);
                        LOG.info(vmInfo.totalString());
                    }

                    LOG.info(PerfTrace.getInstance().summarizeNoException());
                    this.logStatistics();
                }
            }
        }
    }

    private void preCheck() {
        this.preCheckInit();
        this.adjustChannelNumber();

        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }
        this.preCheckReader();
        this.preCheckWriter();
        LOG.info("PreCheck通过");
    }

    private void preCheckInit() {
        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);

        if (this.jobId < 0) {
            LOG.info("Set jobId = 0");
            this.jobId = 0;
            this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID,
                    this.jobId);
        }

        Thread.currentThread().setName("job-" + this.jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        this.jobReader = this.preCheckReaderInit(jobPluginCollector);
        this.jobWriter = this.preCheckWriterInit(jobPluginCollector);
    }

    private Reader.Job preCheckReaderInit(JobPluginCollector jobPluginCollector) {
        this.readerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(
                PluginType.READER, this.readerPluginName);

        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER + ".dryRun", true);

        // 设置reader的jobConfig
        jobReader.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
        // 设置reader的readerConfig
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobReader.setJobPluginCollector(jobPluginCollector);

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }


    private Writer.Job preCheckWriterInit(JobPluginCollector jobPluginCollector) {
        this.writerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(
                PluginType.WRITER, this.writerPluginName);

        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER + ".dryRun", true);

        // 设置writer的jobConfig
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
        // 设置reader的readerConfig
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void preCheckReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do preCheck work .",
                this.readerPluginName));
        this.jobReader.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void preCheckWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do preCheck work .",
                this.writerPluginName));
        this.jobWriter.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * reader和writer的初始化
     */
    private void init() {
        // 1
        this.jobId = this.configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);

        if (this.jobId < 0) {
            LOG.info("Set jobId = 0");
            this.jobId = 0;
            this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID,
                    this.jobId);
        }

        // 设置当前线程名称 (main 线程)
        Thread.currentThread().setName("job-" + this.jobId);

        // 创建 DefaultJobPluginCollector
        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                // 默认为 null
                this.getContainerCommunicator());

        // 必须先 Reader ，后Writer
        // 解析 Reader Writer 类似
        // 解析 StreamReader 返回 StreamReader.job
        this.jobReader = this.initJobReader(jobPluginCollector);
        // 解析 StreamWriter 返回 StreamWriter.job
        this.jobWriter = this.initJobWriter(jobPluginCollector);
    }

    private void prepare() {
        // 调用 StreamReader$Job prepare()
        this.prepareJobReader();
        // 调用 StreamWriter$Job prepare()
        this.prepareJobWriter();
    }

    /**
     * 前置处理器
     */
    private void preHandle() {
        // 获取 handlerPluginType
        String handlerPluginTypeStr = this.configuration.getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINTYPE);
        // 默认没有配置 handler plugin type 直接返回
        if (!StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            return;
        }
        PluginType handlerPluginType;
        try {
            // 根据 handlerPluginType[READER("reader"), TRANSFORMER("transformer"), WRITER("writer"), HANDLER("handler")]
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job preHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        // 获取 handlerPluginName
        String handlerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(
                handlerPluginType, handlerPluginName);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        handler.setJobPluginCollector(jobPluginCollector);

        //todo configuration的安全性，将来必须保证
        handler.preHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        LOG.info("After PreHandler: \n" + Engine.filterJobConfiguration(configuration) + "\n");
    }

    private void postHandle() {
        String handlerPluginTypeStr = this.configuration.getString(
                CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINTYPE);

        if (!StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            return;
        }
        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job postHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(
                handlerPluginType, handlerPluginName);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                this.getContainerCommunicator());
        handler.setJobPluginCollector(jobPluginCollector);

        handler.postHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }


    /**
     * 执行reader和writer最细粒度的切分，需要注意的是，writer的切分结果要参照reader的切分结果，
     * 达到切分后数目相等，才能满足1：1的通道模型，所以这里可以将reader和writer的配置整合到一起，
     * 然后，为避免顺序给读写端带来长尾影响，将整合的结果shuffler掉
     */
    private int split() {
        // 调整 ChannelNumber 默认情况下为 5 个 channel 也即 needChannelNumber = 5
        // 如果设置 job 内容 设置 job.setting.speed.byte 或者 job.setting.speed.record
        // 那么就需要计算 needChannelNumber 也即限流计算
        this.adjustChannelNumber();

        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }

        // 基于场景驱动 needChannelNumber = 5
        // 执行切分 本质还是将配置参数进行拷贝 (Reader & Writer 切分都是类似的)
        /**
         * 其中一个 Configuration 如下 (5 个 Configuration 都是一样的内容)
         * {
         * "column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * "sliceRecordCount": 10
         * }
         */
        List<Configuration> readerTaskConfigs = this
                .doReaderSplit(this.needChannelNumber);

        int taskNumber = readerTaskConfigs.size();
        List<Configuration> writerTaskConfigs = this
                .doWriterSplit(taskNumber);

        // 默认为 null
        List<Configuration> transformerList = this.configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);

        LOG.debug("transformer configuration: " + JSON.toJSONString(transformerList));
        /**
         * 输入是reader和writer的parameter list，输出是content下面元素的list
         */
        /**
         * 比如其中一个 默认 5 个 都是相同的内容
         * {
         * 	"reader": {
         * 		"name": "streamreader",
         * 		"parameter": {
         * 			"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 			"sliceRecordCount": 10
         *                }* 	},
         * 	"taskId": 0,
         * 	"writer": {
         * 		"name": "streamwriter",
         * 		"parameter": {
         * 			"encoding": "UTF-8",
         * 			"print": true
         *        }
         *    }
         * }
         */
        List<Configuration> contentConfig = mergeReaderAndWriterTaskConfigs(
                readerTaskConfigs, writerTaskConfigs, transformerList);

        LOG.debug("contentConfig configuration: " + JSON.toJSONString(contentConfig));

        // 覆盖掉 job.content 默认五个 如下内容
        /**
         * {
         * 	"reader": {
         * 		"name": "streamreader",
         * 		"parameter": {
         * 			"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 			"sliceRecordCount": 10
         *                }* 	},
         * 	"taskId": 0,
         * 	"writer": {
         * 		"name": "streamwriter",
         * 		"parameter": {
         * 			"encoding": "UTF-8",
         * 			"print": true
         *        }
         *    }
         * }
         */
        // 覆盖掉之前的 job.content
        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentConfig);

        return contentConfig.size();
    }

    /**
     * 调整 ChannelNumber
     */
    private void adjustChannelNumber() {
        int needChannelNumberByByte = Integer.MAX_VALUE;
        int needChannelNumberByRecord = Integer.MAX_VALUE;

        // 默认为 false (没有设置)
        boolean isByteLimit = (this.configuration.getInt(
                // job.setting.speed.byte
                CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE,
                0) > 0);
        if (isByteLimit) {
            long globalLimitedByteSpeed = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 10 * 1024 * 1024);

            // 在byte流控情况下，单个Channel流量最大值必须设置，否则报错！
            // core.transport.channel.speed.byte 默认 -1
            Long channelLimitedByteSpeed = this.configuration
                    .getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.CONFIG_ERROR,
                        "在有总bps限速条件下，单个channel的bps值不能为空，也不能为非正数");
            }

            // 计算需要 channel 个数
            needChannelNumberByByte =
                    (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
            needChannelNumberByByte =
                    needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
            LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
        }

        // 默认为 false (没有设置)
        boolean isRecordLimit = (this.configuration.getInt(
                // job.setting.speed.record
                CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 0)) > 0;
        if (isRecordLimit) {
            long globalLimitedRecordSpeed = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 100000);

            // core.transport.channel.speed.record 默认 -1
            Long channelLimitedRecordSpeed = this.configuration.getLong(
                    CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
            if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                        "在有总tps限速条件下，单个channel的tps值不能为空，也不能为非正数");
            }

            needChannelNumberByRecord =
                    (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord =
                    needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
            LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
        }

        // 取较小值
        this.needChannelNumber = Math.min(needChannelNumberByByte, needChannelNumberByRecord);

        // 如果从byte或record上设置了needChannelNumber则退出
        if (this.needChannelNumber < Integer.MAX_VALUE) {
            return;
        }

        // 默认为 5 Channel isChannelLimit = true
        boolean isChannelLimit = (this.configuration.getInt(
                // job.setting.speed.channel
                CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL, 0) > 0);
        if (isChannelLimit) {
            // 默认为 5 Channel
            this.needChannelNumber = this.configuration.getInt(
                    CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);

            LOG.info("Job set Channel-Number to " + this.needChannelNumber
                    + " channels.");

            return;
        }

        throw DataXException.asDataXException(
                FrameworkErrorCode.CONFIG_ERROR,
                "Job运行速度必须设置");
    }

    /**
     * schedule首先完成的工作是把上一步reader和writer split的结果整合到具体taskGroupContainer中,
     * 同时不同的执行模式调用不同的调度策略，将所有任务调度起来
     */
    private void schedule() {
        /**
         * 这里的全局speed和每个channel的速度设置为B/s
         */
        // 默认为 5
        int channelsPerTaskGroup = this.configuration.getInt(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, 5);
        // 默认为 5
        // 此时 job.content 已经被切分的内容覆盖掉 也即 job.content 里面的内容是很多个相关的内容
        /**
         * 比如
         * {
         *     "reader":{
         *         "name":"streamreader",
         *         "parameter":{
         *             "column":[
         *                 "{\"type\":\"long\",\"value\":\"10\"}",
         *                 "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"
         *             ],
         *             "sliceRecordCount":10
         *         }
         *     },
         *     "taskId":0,
         *     "writer":{
         *         "name":"streamwriter",
         *         "parameter":{
         *             "encoding":"UTF-8",
         *             "print":true
         *         }
         *     }
         * }
         *
         */
        int taskNumber = this.configuration.getList(
                CoreConstant.DATAX_JOB_CONTENT).size();

        // 默认为 5
        this.needChannelNumber = Math.min(this.needChannelNumber, taskNumber);
        // 设置记录 ChannelNumber = 5
        // 单例模式
        PerfTrace.getInstance().setChannelNumber(needChannelNumber);

        /**
         * 通过获取配置信息得到每个taskGroup需要运行哪些tasks任务
         */
        /**
         * 基于场景驱动 以及配置信息 划分了 一个 TaskGroup 对应的 5 个 Channel
         * 所以 taskGroupConfigs 集合只有一个 TaskGroup 配置信息
         * 比如如下信息：
         */

        /**
         * {
         * 	"common": {
         * 		"column": {
         * 			"dateFormat": "yyyy-MM-dd",
         * 			"datetimeFormat": "yyyy-MM-dd HH:mm:ss",
         * 			"encoding": "utf-8",
         * 			"extraFormats": ["yyyyMMdd"],
         * 			"timeFormat": "HH:mm:ss",
         * 			"timeZone": "GMT+8"
         *                }* 	},
         * 	"core": {
         * 		"container": {
         * 			"job": {
         * 				"id": 1,
         * 				"reportInterval": 10000
         *            },
         * 			"taskGroup": {
         * 				"channel": 5,
         * 				"id": 0
         *            },
         * 			"trace": {
         * 				"enable": "false"
         *            }
         *        },
         * 		"dataXServer": {
         * 			"address": "http://localhost:7001/api",
         * 			"reportDataxLog": false,
         * 			"reportPerfLog": false,
         * 			"timeout": 10000
         *        },
         * 		"statistics": {
         * 			"collector": {
         * 				"plugin": {
         * 					"maxDirtyNumber": 10,
         * 					"taskClass": "com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector"
         *                }
         *            }
         *        },
         * 		"transport": {
         * 			"channel": {
         * 				"byteCapacity": 67108864,
         * 				"capacity": 512,
         * 				"class": "com.alibaba.datax.core.transport.channel.memory.MemoryChannel",
         * 				"flowControlInterval": 20,
         * 				"speed": {
         * 					"byte": -1,
         * 					"record": -1
         *                }
         *            },
         * 			"exchanger": {
         * 				"bufferSize": 32,
         * 				"class": "com.alibaba.datax.core.plugin.BufferedRecordExchanger"
         *            }
         *        }
         *    },
         * 	"entry": {
         * 		"jvm": "-Xms1G -Xmx1G"
         *    },
         * 	"job": {
         * 		"content": [{
         * 			"reader": {
         * 				"name": "streamreader",
         * 				"parameter": {
         * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 					"sliceRecordCount": 10
         *                }
         *            },
         * 			"taskId": 0,
         * 			"writer": {
         * 				"name": "streamwriter",
         * 				"parameter": {
         * 					"encoding": "UTF-8",
         * 					"print": true
         *                }
         *            }
         *        }, {
         * 			"reader": {
         * 				"name": "streamreader",
         * 				"parameter": {
         * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 					"sliceRecordCount": 10
         *                }
         *            },
         * 			"taskId": 3,
         * 			"writer": {
         * 				"name": "streamwriter",
         * 				"parameter": {
         * 					"encoding": "UTF-8",
         * 					"print": true
         *                }
         *            }
         *        }, {
         * 			"reader": {
         * 				"name": "streamreader",
         * 				"parameter": {
         * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 					"sliceRecordCount": 10
         *                }
         *            },
         * 			"taskId": 1,
         * 			"writer": {
         * 				"name": "streamwriter",
         * 				"parameter": {
         * 					"encoding": "UTF-8",
         * 					"print": true
         *                }
         *            }
         *        }, {
         * 			"reader": {
         * 				"name": "streamreader",
         * 				"parameter": {
         * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 					"sliceRecordCount": 10
         *                }
         *            },
         * 			"taskId": 4,
         * 			"writer": {
         * 				"name": "streamwriter",
         * 				"parameter": {
         * 					"encoding": "UTF-8",
         * 					"print": true
         *                }
         *            }
         *        }, {
         * 			"reader": {
         * 				"name": "streamreader",
         * 				"parameter": {
         * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 					"sliceRecordCount": 10
         *                }
         *            },
         * 			"taskId": 2,
         * 			"writer": {
         * 				"name": "streamwriter",
         * 				"parameter": {
         * 					"encoding": "UTF-8",
         * 					"print": true
         *                }
         *            }
         *        }],
         * 		"setting": {
         * 			"speed": {
         * 				"channel": 5
         *            }
         *        }
         *    },
         * 	"plugin": {
         * 		"reader": {
         * 			"streamreader": {
         * 				"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
         * 				"description": {
         * 					"mechanism": "use datax framework to transport data from stream.",
         * 					"useScene": "only for developer test.",
         * 					"warn": "Never use it in your real job."
         *                },
         * 				"developer": "alibaba",
         * 				"name": "streamreader",
         * 				"path": "D:\\app\\datax\\plugin\\reader\\streamreader"
         *            }
         *        },
         * 		"writer": {
         * 			"streamwriter": {
         * 				"class": "com.alibaba.datax.plugin.writer.streamwriter.StreamWriter",
         * 				"description": {
         * 					"mechanism": "use datax framework to transport data to stream.",
         * 					"useScene": "only for developer test.",
         * 					"warn": "Never use it in your real job."
         *                },
         * 				"developer": "alibaba",
         * 				"name": "streamwriter",
         * 				"path": "D:\\app\\datax\\plugin\\writer\\streamwriter"
         *            }
         *        }
         *    }
         * }
         */
        List<Configuration> taskGroupConfigs = JobAssignUtil.assignFairly(
                this.configuration,
                this.needChannelNumber,
                channelsPerTaskGroup);

        LOG.info("Scheduler starts [{}] taskGroups.", taskGroupConfigs.size());

        ExecuteMode executeMode = null;
        AbstractScheduler scheduler;
        try {
            executeMode = ExecuteMode.STANDALONE;
            // 创建 StandAloneScheduler
            /**
             * 收集 metric 对象 StandAloneJobContainerCommunicator(ProcessInnerCollector, ProcessInnerReporter)
             *                                    -> JobContainer
             * StandAloneJobContainerCommunicator
             *                                    -> StandaloneScheduler extend ProcessInnerScheduler
             *
             */
            scheduler = initStandaloneScheduler(this.configuration);

            // 设置 executeMode  STANDALONE
            for (Configuration taskGroupConfig : taskGroupConfigs) {
                taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, executeMode.getValue());
            }

            LOG.info("Running by {} Mode.", executeMode);

            this.startTransferTimeStamp = System.currentTimeMillis();

            // 调度执行 TaskGroup
            /**
             *{
             * 	"common": {
             * 		"column": {
             * 			"dateFormat": "yyyy-MM-dd",
             * 			"datetimeFormat": "yyyy-MM-dd HH:mm:ss",
             * 			"encoding": "utf-8",
             * 			"extraFormats": ["yyyyMMdd"],
             * 			"timeFormat": "HH:mm:ss",
             * 			"timeZone": "GMT+8"
             *                }* 	},
             * 	"core": {
             * 		"container": {
             * 			"job": {
             * 				"id": 1,
             * 				"mode": "standalone",
             * 				"reportInterval": 10000
             *            },
             * 			"taskGroup": {
             * 				"channel": 5,
             * 				"id": 0
             *            },
             * 			"trace": {
             * 				"enable": "false"
             *            }
             *        },
             * 		"dataXServer": {
             * 			"address": "http://localhost:7001/api",
             * 			"reportDataxLog": false,
             * 			"reportPerfLog": false,
             * 			"timeout": 10000
             *        },
             * 		"statistics": {
             * 			"collector": {
             * 				"plugin": {
             * 					"maxDirtyNumber": 10,
             * 					"taskClass": "com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector"
             *                }
             *            }
             *        },
             * 		"transport": {
             * 			"channel": {
             * 				"byteCapacity": 67108864,
             * 				"capacity": 512,
             * 				"class": "com.alibaba.datax.core.transport.channel.memory.MemoryChannel",
             * 				"flowControlInterval": 20,
             * 				"speed": {
             * 					"byte": -1,
             * 					"record": -1
             *                }
             *            },
             * 			"exchanger": {
             * 				"bufferSize": 32,
             * 				"class": "com.alibaba.datax.core.plugin.BufferedRecordExchanger"
             *            }
             *        }
             *    },
             * 	"entry": {
             * 		"jvm": "-Xms1G -Xmx1G"
             *    },
             * 	"job": {
             * 		"content": [{
             * 			"reader": {
             * 				"name": "streamreader",
             * 				"parameter": {
             * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
             * 					"sliceRecordCount": 10
             *                }
             *            },
             * 			"taskId": 4,
             * 			"writer": {
             * 				"name": "streamwriter",
             * 				"parameter": {
             * 					"encoding": "UTF-8",
             * 					"print": true
             *                }
             *            }
             *        }, {
             * 			"reader": {
             * 				"name": "streamreader",
             * 				"parameter": {
             * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
             * 					"sliceRecordCount": 10
             *                }
             *            },
             * 			"taskId": 1,
             * 			"writer": {
             * 				"name": "streamwriter",
             * 				"parameter": {
             * 					"encoding": "UTF-8",
             * 					"print": true
             *                }
             *            }
             *        }, {
             * 			"reader": {
             * 				"name": "streamreader",
             * 				"parameter": {
             * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
             * 					"sliceRecordCount": 10
             *                }
             *            },
             * 			"taskId": 3,
             * 			"writer": {
             * 				"name": "streamwriter",
             * 				"parameter": {
             * 					"encoding": "UTF-8",
             * 					"print": true
             *                }
             *            }
             *        }, {
             * 			"reader": {
             * 				"name": "streamreader",
             * 				"parameter": {
             * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
             * 					"sliceRecordCount": 10
             *                }
             *            },
             * 			"taskId": 2,
             * 			"writer": {
             * 				"name": "streamwriter",
             * 				"parameter": {
             * 					"encoding": "UTF-8",
             * 					"print": true
             *                }
             *            }
             *        }, {
             * 			"reader": {
             * 				"name": "streamreader",
             * 				"parameter": {
             * 					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
             * 					"sliceRecordCount": 10
             *                }
             *            },
             * 			"taskId": 0,
             * 			"writer": {
             * 				"name": "streamwriter",
             * 				"parameter": {
             * 					"encoding": "UTF-8",
             * 					"print": true
             *                }
             *            }
             *        }],
             * 		"setting": {
             * 			"speed": {
             * 				"channel": 5
             *            }
             *        }
             *    },
             * 	"plugin": {
             * 		"reader": {
             * 			"streamreader": {
             * 				"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
             * 				"description": {
             * 					"mechanism": "use datax framework to transport data from stream.",
             * 					"useScene": "only for developer test.",
             * 					"warn": "Never use it in your real job."
             *                },
             * 				"developer": "alibaba",
             * 				"name": "streamreader",
             * 				"path": "D:\\app\\datax\\plugin\\reader\\streamreader"
             *            }
             *        },
             * 		"writer": {
             * 			"streamwriter": {
             * 				"class": "com.alibaba.datax.plugin.writer.streamwriter.StreamWriter",
             * 				"description": {
             * 					"mechanism": "use datax framework to transport data to stream.",
             * 					"useScene": "only for developer test.",
             * 					"warn": "Never use it in your real job."
             *                },
             * 				"developer": "alibaba",
             * 				"name": "streamwriter",
             * 				"path": "D:\\app\\datax\\plugin\\writer\\streamwriter"
             *            }
             *        }
             *    }
             * }
             */
            scheduler.schedule(taskGroupConfigs);

            this.endTransferTimeStamp = System.currentTimeMillis();
        } catch (Exception e) {
            LOG.error("运行scheduler 模式[{}]出错.", executeMode);
            this.endTransferTimeStamp = System.currentTimeMillis();
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        }

        /**
         * 检查任务执行情况
         */
        this.checkLimit();
    }

    private AbstractScheduler initStandaloneScheduler(Configuration configuration) {
        // 创建 StandAloneJobContainerCommunicator
        AbstractContainerCommunicator containerCommunicator = new StandAloneJobContainerCommunicator(configuration);
        // JobContainer 设置 StandAloneJobContainerCommunicator
        super.setContainerCommunicator(containerCommunicator);

        // 创建 StandAloneScheduler
        return new StandAloneScheduler(containerCommunicator);
    }

    private void post() {
        this.postJobWriter();
        this.postJobReader();
    }

    private void destroy() {
        if (this.jobWriter != null) {
            this.jobWriter.destroy();
            this.jobWriter = null;
        }
        if (this.jobReader != null) {
            this.jobReader.destroy();
            this.jobReader = null;
        }
    }

    private void logStatistics() {
        long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000;
        long transferCosts = (this.endTransferTimeStamp - this.startTransferTimeStamp) / 1000;
        if (0L == transferCosts) {
            transferCosts = 1L;
        }

        if (super.getContainerCommunicator() == null) {
            return;
        }

        Communication communication = super.getContainerCommunicator().collect();
        communication.setTimestamp(this.endTimeStamp);

        Communication tempComm = new Communication();
        tempComm.setTimestamp(this.startTransferTimeStamp);

        Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);

        // 字节速率
        long byteSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES)
                / transferCosts;

        long recordSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS)
                / transferCosts;

        reportCommunication.setLongCounter(CommunicationTool.BYTE_SPEED, byteSpeedPerSecond);
        reportCommunication.setLongCounter(CommunicationTool.RECORD_SPEED, recordSpeedPerSecond);

        super.getContainerCommunicator().report(reportCommunication);


        LOG.info(String.format(
                "\n" + "%-26s: %-18s\n" + "%-26s: %-18s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n",
                "任务启动时刻",
                dateFormat.format(startTimeStamp),

                "任务结束时刻",
                dateFormat.format(endTimeStamp),

                "任务总计耗时",
                String.valueOf(totalCosts) + "s",
                "任务平均流量",
                StrUtil.stringify(byteSpeedPerSecond)
                        + "/s",
                "记录写入速度",
                String.valueOf(recordSpeedPerSecond)
                        + "rec/s", "读出记录总数",
                String.valueOf(CommunicationTool.getTotalReadRecords(communication)),
                "读写失败总数",
                String.valueOf(CommunicationTool.getTotalErrorRecords(communication))
        ));

        if (communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS) > 0
                || communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS) > 0
                || communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS) > 0) {
            LOG.info(String.format(
                    "\n" + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n",
                    "Transformer成功记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS),

                    "Transformer失败记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS),

                    "Transformer过滤记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS)
            ));
        }


    }

    /**
     * reader job的初始化，返回Reader.Job
     *
     * @return
     */
    private Reader.Job initJobReader(
            JobPluginCollector jobPluginCollector) {
        // 获取 readerPluginName = streamreader
        this.readerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);

        // 加载 JarLoader
        /**
         * getJarLoader() 这个方法将会执行初始化 JarLoader
         * 比如 getJarLoader(reader,streamreader) 那么进行加载 plugin.reader.streamreader 对应的
         * 配置文件 其中该配置文件就是 plugin.json 有一个配置 path, 那么将会创建一个 JarLoader 进行加载 path 路径下
         * 所有的 jar, 并且将 key = plugin.reader.streamreader value = JarLoader 存放到一个 Map
         * 其他的 writer、transform 类似
         */
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));

        // 基于场景驱动 jobReader 就是 com.alibaba.datax.plugin.reader.streamreader.StreamReader
        // 创建并初始化静态内部类 Job
        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(
                PluginType.READER, this.readerPluginName);

        // 设置 reader 插件的 pluginJobConf
        jobReader.setPluginJobConf(this.configuration.getConfiguration(
                // job.content[0].reader.parameter
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        // 设置 reader 插件的 peerPluginJobConf
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration(
                // job.content[0].writer.parameter
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

        // 设置 reader 插件的 DefaultJobPluginCollector
        jobReader.setJobPluginCollector(jobPluginCollector);

        // 基于场景驱动调用 StreamReader 内部类的 Job.init()
        jobReader.init();

        // 恢复当前类加载器
        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    /**
     * writer job的初始化，返回Writer.Job
     * writer job 的初始化跟 reader job 初始化类似
     *
     * @return
     */
    private Writer.Job initJobWriter(
            JobPluginCollector jobPluginCollector) {
        // 获取 writerPluginName 基于场景驱动等于 streamwriter
        this.writerPluginName = this.configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        // 这种方式根据 reader 类似
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        // 反射方式创建 StreamWriter$Job
        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(
                PluginType.WRITER, this.writerPluginName);

        // 设置 writer 的 pluginJobConf
        jobWriter.setPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));

        // 设置 writer 的 peerPluginJobConf
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);
        jobWriter.init();
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void prepareJobReader() {
        // 设置当前线程执行 reader 对应的 JarLoader
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do prepare work .",
                this.readerPluginName));
        // 调用 StreamReader Job.prepare()
        this.jobReader.prepare();
        // 恢复当前线程一开始的类加载器
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void prepareJobWriter() {
        // 设置当前线程执行 writer 对应的 JarLoader
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do prepare work .",
                this.writerPluginName));
        // 调用 StreamWriter Job.prepare()
        this.jobWriter.prepare();
        // 恢复当前线程一开始的类加载器
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    // TODO: 如果源头就是空数据
    private List<Configuration> doReaderSplit(int adviceNumber) {
        // 基于场景驱动 adviceNumber = 5
        // 类加载器设置
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        // 切分的本质就是拷贝配置信息 (也就是配置参数)
        /**
         * 比如
         * {
         * "column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * "sliceRecordCount": 10
         * }
         */
        List<Configuration> readerSlicesConfigs =
                // 调用 StreamReader Job.split(5)
                this.jobReader.split(adviceNumber);

        if (readerSlicesConfigs == null || readerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "reader切分的task数目不能小于等于0");
        }
        LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.",
                this.readerPluginName, readerSlicesConfigs.size());
        // 类加载器恢复
        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return readerSlicesConfigs;
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));

        List<Configuration> writerSlicesConfigs = this.jobWriter
                .split(readerTaskNumber);
        if (writerSlicesConfigs == null || writerSlicesConfigs.size() <= 0) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    "writer切分的task不能小于等于0");
        }
        LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.",
                this.writerPluginName, writerSlicesConfigs.size());
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return writerSlicesConfigs;
    }

    /**
     * 按顺序整合reader和writer的配置，这里的顺序不能乱！ 输入是reader、writer级别的配置，输出是一个完整task的配置
     *
     * @param readerTasksConfigs
     * @param writerTasksConfigs
     * @return
     */
    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs) {
        return mergeReaderAndWriterTaskConfigs(readerTasksConfigs, writerTasksConfigs, null);
    }

    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            // job.content[0].reader.parameter 内容 (默认副本 5 也即channel 个数)
            List<Configuration> readerTasksConfigs,
            // job.content[0].writer.parameter 内容 (默认副本 5 也即channel 个数)
            List<Configuration> writerTasksConfigs,
            // 默认 null
            List<Configuration> transformerConfigs) {
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].",
                            readerTasksConfigs.size(), writerTasksConfigs.size())
            );
        }

        // 将 Reader & Writer 部署信息合并
        List<Configuration> contentConfigs = new ArrayList<Configuration>();
        for (int i = 0; i < readerTasksConfigs.size(); i++) {
            Configuration taskConfig = Configuration.newDefault();
            // reader.name = streamreader
            taskConfig.set(CoreConstant.JOB_READER_NAME,
                    this.readerPluginName);
            taskConfig.set(CoreConstant.JOB_READER_PARAMETER,
                    readerTasksConfigs.get(i));
            // writer.name = streamwriter
            taskConfig.set(CoreConstant.JOB_WRITER_NAME,
                    this.writerPluginName);
            taskConfig.set(CoreConstant.JOB_WRITER_PARAMETER,
                    writerTasksConfigs.get(i));

            if (transformerConfigs != null && transformerConfigs.size() > 0) {
                taskConfig.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
            }

            // 设置 TASK_ID
            taskConfig.set(CoreConstant.TASK_ID, i);
            contentConfigs.add(taskConfig);
        }

        /**
         * 比如其中一个 默认 5 个 都是相同的内容 (taskId 不同)
         * {
         * 	"reader": {
         * 		"name": "streamreader",
         * 		"parameter": {
         * 			"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 			"sliceRecordCount": 10
         *                }
         *         },
         * 	"taskId": 0,
         * 	"writer": {
         * 		"name": "streamwriter",
         * 		"parameter": {
         * 			"encoding": "UTF-8",
         * 			"print": true
         *        }
         *    }
         * }
         */
        return contentConfigs;
    }

    /**
     * 这里比较复杂，分两步整合 1. tasks到channel 2. channel到taskGroup
     * 合起来考虑，其实就是把tasks整合到taskGroup中，需要满足计算出的channel数，同时不能多起channel
     * <p/>
     * example:
     * <p/>
     * 前提条件： 切分后是1024个分表，假设用户要求总速率是1000M/s，每个channel的速率的3M/s，
     * 每个taskGroup负责运行7个channel
     * <p/>
     * 计算： 总channel数为：1000M/s / 3M/s =
     * 333个，为平均分配，计算可知有308个每个channel有3个tasks，而有25个每个channel有4个tasks，
     * 需要的taskGroup数为：333 / 7 =
     * 47...4，也就是需要48个taskGroup，47个是每个负责7个channel，有4个负责1个channel
     * <p/>
     * 处理：我们先将这负责4个channel的taskGroup处理掉，逻辑是：
     * 先按平均为3个tasks找4个channel，设置taskGroupId为0，
     * 接下来就像发牌一样轮询分配task到剩下的包含平均channel数的taskGroup中
     * <p/>
     * TODO delete it
     *
     * @param averTaskPerChannel
     * @param channelNumber
     * @param channelsPerTaskGroup
     * @return 每个taskGroup独立的全部配置
     */
    @SuppressWarnings("serial")
    private List<Configuration> distributeTasksToTaskGroup(
            int averTaskPerChannel, int channelNumber,
            int channelsPerTaskGroup) {
        Validate.isTrue(averTaskPerChannel > 0 && channelNumber > 0
                        && channelsPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");
        List<Configuration> taskConfigs = this.configuration
                .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        int taskGroupNumber = channelNumber / channelsPerTaskGroup;
        int leftChannelNumber = channelNumber % channelsPerTaskGroup;
        if (leftChannelNumber > 0) {
            taskGroupNumber += 1;
        }

        /**
         * 如果只有一个taskGroup，直接打标返回
         */
        if (taskGroupNumber == 1) {
            final Configuration taskGroupConfig = this.configuration.clone();
            /**
             * configure的clone不能clone出
             */
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, this.configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT));
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    channelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 0);
            return new ArrayList<Configuration>() {
                {
                    add(taskGroupConfig);
                }
            };
        }

        List<Configuration> taskGroupConfigs = new ArrayList<Configuration>();
        /**
         * 将每个taskGroup中content的配置清空
         */
        for (int i = 0; i < taskGroupNumber; i++) {
            Configuration taskGroupConfig = this.configuration.clone();
            List<Configuration> taskGroupJobContent = taskGroupConfig
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
            taskGroupJobContent.clear();
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);

            taskGroupConfigs.add(taskGroupConfig);
        }

        int taskConfigIndex = 0;
        int channelIndex = 0;
        int taskGroupConfigIndex = 0;

        /**
         * 先处理掉taskGroup包含channel数不是平均值的taskGroup
         */
        if (leftChannelNumber > 0) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            for (; channelIndex < leftChannelNumber; channelIndex++) {
                for (int i = 0; i < averTaskPerChannel; i++) {
                    List<Configuration> taskGroupJobContent = taskGroupConfig
                            .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                    taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                    taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT,
                            taskGroupJobContent);
                }
            }

            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    leftChannelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,
                    taskGroupConfigIndex++);
        }

        /**
         * 下面需要轮询分配，并打上channel数和taskGroupId标记
         */
        int equalDivisionStartIndex = taskGroupConfigIndex;
        for (; taskConfigIndex < taskConfigs.size()
                && equalDivisionStartIndex < taskGroupConfigs.size(); ) {
            for (taskGroupConfigIndex = equalDivisionStartIndex; taskGroupConfigIndex < taskGroupConfigs
                    .size() && taskConfigIndex < taskConfigs.size(); taskGroupConfigIndex++) {
                Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
                List<Configuration> taskGroupJobContent = taskGroupConfig
                        .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                taskGroupConfig.set(
                        CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);
            }
        }

        for (taskGroupConfigIndex = equalDivisionStartIndex;
             taskGroupConfigIndex < taskGroupConfigs.size(); ) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    channelsPerTaskGroup);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,
                    taskGroupConfigIndex++);
        }

        return taskGroupConfigs;
    }

    private void postJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, this.readerPluginName));
        LOG.info("DataX Reader.Job [{}] do post work.",
                this.readerPluginName);
        this.jobReader.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void postJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, this.writerPluginName));
        LOG.info("DataX Writer.Job [{}] do post work.",
                this.writerPluginName);
        this.jobWriter.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * 检查最终结果是否超出阈值，如果阈值设定小于1，则表示百分数阈值，大于1表示条数阈值。
     *
     * @param
     */
    private void checkLimit() {
        Communication communication = super.getContainerCommunicator().collect();
        errorLimit.checkRecordLimit(communication);
        errorLimit.checkPercentageLimit(communication);
    }

    /**
     * 调用外部hook
     */
    private void invokeHooks() {
        Communication comm = super.getContainerCommunicator().collect();
        HookInvoker invoker = new HookInvoker(CoreConstant.DATAX_HOME + "/hook", configuration, comm.getCounter());
        invoker.invokeAll();
    }
}
