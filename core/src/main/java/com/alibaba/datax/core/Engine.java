package com.alibaba.datax.core;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine {
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    private static String RUNTIME_MODE;

    /* check job model (job/task) first */

    /**
     *{
     *     "common":{
     *         "column":{
     *             "dateFormat":"yyyy-MM-dd",
     *             "datetimeFormat":"yyyy-MM-dd HH:mm:ss",
     *             "encoding":"utf-8",
     *             "extraFormats":[
     *                 "yyyyMMdd"
     *             ],
     *             "timeFormat":"HH:mm:ss",
     *             "timeZone":"GMT+8"
     *         }
     *     },
     *     "core":{
     *         "container":{
     *             "job":{
     *                 "id":1,
     *                 "reportInterval":10000
     *             },
     *             "taskGroup":{
     *                 "channel":5
     *             },
     *             "trace":{
     *                 "enable":"false"
     *             }
     *         },
     *         "dataXServer":{
     *             "address":"http://localhost:8080/monitor/job",
     *             "reportDataxLog":true,
     *             "reportPerfLog":false,
     *             "timeout":10000
     *         },
     *         "redis":{
     *             "host":"hj108",
     *             "port":6379
     *         },
     *         "statistics":{
     *             "collector":{
     *                 "plugin":{
     *                     "maxDirtyNumber":10,
     *                     "taskClass":"com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector"
     *                 }
     *             }
     *         },
     *         "transport":{
     *             "channel":{
     *                 "byteCapacity":67108864,
     *                 "capacity":512,
     *                 "class":"com.alibaba.datax.core.transport.channel.memory.MemoryChannel",
     *                 "flowControlInterval":20,
     *                 "speed":{
     *                     "byte":-1,
     *                     "record":-1
     *                 }
     *             },
     *             "exchanger":{
     *                 "bufferSize":32,
     *                 "class":"com.alibaba.datax.core.plugin.BufferedRecordExchanger"
     *             }
     *         }
     *     },
     *     "entry":{
     *         "jvm":"-Xms1G -Xmx1G"
     *     },
     *     "job":{
     *         "content":[
     *             {
     *                 "reader":{
     *                     "name":"streamreader",
     *                     "parameter":{
     *                         "column":[
     *                             {
     *                                 "type":"long",
     *                                 "value":"10"
     *                             },
     *                             {
     *                                 "type":"string",
     *                                 "value":"hello，你好，世界-DataX"
     *                             }
     *                         ],
     *                         "sliceRecordCount":10
     *                     }
     *                 },
     *                 "writer":{
     *                     "name":"streamwriter",
     *                     "parameter":{
     *                         "encoding":"UTF-8",
     *                         "print":true
     *                     }
     *                 }
     *             }
     *         ],
     *         "setting":{
     *             "speed":{
     *                 "channel":5
     *             }
     *         }
     *     },
     *     "plugin":{
     *         "reader":{
     *             "streamreader":{
     *                 "class":"com.alibaba.datax.plugin.reader.streamreader.StreamReader",
     *                 "description":{
     *                     "mechanism":"use datax framework to transport data from stream.",
     *                     "useScene":"only for developer test.",
     *                     "warn":"Never use it in your real job."
     *                 },
     *                 "developer":"alibaba",
     *                 "name":"streamreader",
     *                 "path":"D:\\app\\datax\\plugin\\reader\\streamreader"
     *             }
     *         },
     *         "writer":{
     *             "streamwriter":{
     *                 "class":"com.alibaba.datax.plugin.writer.streamwriter.StreamWriter",
     *                 "description":{
     *                     "mechanism":"use datax framework to transport data to stream.",
     *                     "useScene":"only for developer test.",
     *                     "warn":"Never use it in your real job."
     *                 },
     *                 "developer":"alibaba",
     *                 "name":"streamwriter",
     *                 "path":"D:\\app\\datax\\plugin\\writer\\streamwriter"
     *             }
     *         }
     *     }
     * }
     */
    public void start(Configuration allConf) {

        // 绑定 column 转换信息
        ColumnCast.bind(allConf);

        /**
         * 初始化 PluginLoader，可以获取各种插件配置
         */
        LoadUtil.bind(allConf);

        // isJob = true
        boolean isJob = !("taskGroup".equalsIgnoreCase(allConf
                // core.container.model 为 null
                .getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));

        // JobContainer 会在schedule后再行进行设置和调整值
        int channelNumber = 0;
        AbstractContainer container;
        long instanceId;
        int taskGroupId = -1;
        // 基于场景驱动 isJob = true 所以会创建 JobContainer
        if (isJob) {
            // 设置 core.container.job.mode = null
            allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, RUNTIME_MODE);
            // 创建 JobContainer
            container = new JobContainer(allConf);
            // 获取 JobId core.container.job.id = 1
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
        } else {
            // 创建 TaskGroupContainer
            container = new TaskGroupContainer(allConf);
            // 获取 JobId
            instanceId = allConf.getLong(
                    CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
            // 获取 TaskGroupId
            taskGroupId = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            // 获取 ChannelNumber
            channelNumber = allConf.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
        }

        // 缺省打开 perfTrace 默认为 true
        boolean traceEnable = allConf.getBool(CoreConstant.DATAX_CORE_CONTAINER_TRACE_ENABLE, true);
        // 默认为 false
        boolean perfReportEnable = allConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_PERFLOG, true);

        //standalone模式的 datax shell任务不进行汇报
        if (instanceId == -1) {
            perfReportEnable = false;
        }

        int priority = 0;
        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        } catch (NumberFormatException e) {
            LOG.warn("prioriy set to 0, because NumberFormatException, the value is: " + System.getProperty("PROIORY"));
        }

        // 默认为 null
        Configuration jobInfoConfig = allConf.getConfiguration(CoreConstant.DATAX_JOB_JOBINFO);

        // 初始化 PerfTrace JVM 只有一个 单例模式
        PerfTrace perfTrace = PerfTrace.getInstance(isJob, instanceId, taskGroupId, priority, traceEnable);
        perfTrace.setJobInfo(jobInfoConfig, perfReportEnable, channelNumber);

        // 启动 JobContainer (如果设置了 taskGroup 对应的配置 则启动 TaskGroupContainer)
        container.start();
    }


    // 注意屏蔽敏感信息
    public static String filterJobConfiguration(final Configuration configuration) {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();

        Configuration jobContent = jobConfWithSetting.getConfiguration("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content", jobContent);

        return jobConfWithSetting.beautify();
    }

    public static Configuration filterSensitiveConfiguration(Configuration configuration) {
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }
        return configuration;
    }

    /**
     * 基于流式打印 Job
     * args = ["-job","D:\project\DataX\stream2stream.json","-jobid","1"]
     *
     * @param args
     * @throws Throwable
     */
    public static void entry(final String[] args) throws Throwable {
        Options options = new Options();
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("mode", true, "Job runtime mode.");

        // 解析参数
        /**
         * 主要解析
         * 1. -job 对应的配置文件
         * 2. -jobid 任务 ID
         * 3. -mode 任务模式
         */
        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);

        // 基于场景驱动 jobPath = D:\project\DataX\stream2stream.json
        String jobPath = cl.getOptionValue("job");

        // 如果用户没有明确指定jobid, 则 datax.py 会指定 jobid 默认值为-1
        String jobIdString = cl.getOptionValue("jobid");
        RUNTIME_MODE = cl.getOptionValue("mode");

        /**
         * 1. 根据 job 解析 JSON 封装成 Configuration 的 Object root 对象
         * 2. 加载 core.json 文件合并到 root 对象 (可以执行覆盖，默认情况下不覆盖)
         * 3. 加载 plugin 下的所有插件匹配目标对应的 reader、writer 封装到 root 对象
         * 备注：root 对象包括了 core、entry、common、plugin、job 四大字段
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
         *                }
         *       },
         * 	"core": {
         * 		"container": {
         * 			"job": {
         * 				"reportInterval": 10000
         *            },
         * 			"taskGroup": {
         * 				"channel": 5
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
         * 					"column": [{
         * 						"type": "long",
         * 						"value": "10"
         *                    }, {
         * 						"type": "string",
         * 						"value": "hello，你好，世界-DataX"
         *                    }],
         * 					"sliceRecordCount": 10
         *                }
         *            },
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
        Configuration configuration = ConfigParser.parse(jobPath);

        // 解析 jobid
        long jobId;
        if (!"-1".equalsIgnoreCase(jobIdString)) {
            // jobid = 1
            jobId = Long.parseLong(jobIdString);
        } else {
            // only for dsc & ds & datax 3 update
            String dscJobUrlPatternString = "/instance/(\\d{1,})/config.xml";
            String dsJobUrlPatternString = "/inner/job/(\\d{1,})/config";
            String dsTaskGroupUrlPatternString = "/inner/job/(\\d{1,})/taskGroup/";
            List<String> patternStringList = Arrays.asList(dscJobUrlPatternString,
                    dsJobUrlPatternString, dsTaskGroupUrlPatternString);
            jobId = parseJobIdFromUrl(patternStringList, jobPath);
        }

        // 默认情况下 isStandAloneMode = false
        boolean isStandAloneMode = "standalone".equalsIgnoreCase(RUNTIME_MODE);
        if (!isStandAloneMode && jobId == -1) {
            // 如果不是 standalone 模式，那么 jobId 一定不能为-1
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "非 standalone 模式必须在 URL 中提供有效的 jobId.");
        }
        // 设置 configuration 对应的 core.container.job.id  = 1
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);

        //打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            /**
             * osInfo:	Oracle Corporation 1.8 25.144-b01
             * 	jvmInfo:	Windows 10 amd64 10.0
             * 	cpu num:	8
             *
             * 	totalPhysicalMemory:	-0.00G
             * 	freePhysicalMemory:	-0.00G
             * 	maxFileDescriptorCount:	-1
             * 	currentOpenFileDescriptorCount:	-1
             *
             * 	GC Names	[PS MarkSweep, PS Scavenge]
             *
             * 	MEMORY_NAME                    | allocation_size                | init_size
             * 	PS Eden Space                  | 1,326.00MB                     | 63.50MB
             * 	Code Cache                     | 240.00MB                       | 2.44MB
             * 	Compressed Class Space         | 1,024.00MB                     | 0.00MB
             * 	PS Survivor Space              | 10.50MB                        | 10.50MB
             * 	PS Old Gen                     | 2,695.00MB                     | 169.50MB
             * 	Metaspace                      | -0.00MB                        | 0.00MB
             */
            LOG.info(vmInfo.toString());
        }

        // 打印核心配置参数
        /**
         * {
         * 	"content":[
         *                {
         * 			"reader":{
         * 				"name":"streamreader",
         * 				"parameter":{
         * 					"column":[
         *                        {
         * 							"type":"long",
         * 							"value":"10"
         *                        },
         *                        {
         * 							"type":"string",
         * 							"value":"hello，你好，世界-DataX"
         *                        }
         * 					],
         * 					"sliceRecordCount":10
         *                }
         *            },
         * 			"writer":{
         * 				"name":"streamwriter",
         * 				"parameter":{
         * 					"encoding":"UTF-8",
         * 					"print":true
         *                }
         *            }
         *        }
         * 	],
         * 	"setting":{
         * 		"speed":{
         * 			"channel":5
         *        }* 	}
         * }
         */
        LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");

        LOG.debug(configuration.toJSON());
        ConfigurationValidate.doValidate(configuration);

        // 创建 Engine 对象
        Engine engine = new Engine();
        // 启动 Engine
        engine.start(configuration);
    }


    /**
     * -1 表示未能解析到 jobId
     * <p>
     * only for dsc & ds & datax 3 update
     */
    private static long parseJobIdFromUrl(List<String> patternStringList, String url) {
        long result = -1;
        for (String patternString : patternStringList) {
            result = doParseJobIdFromUrl(patternString, url);
            if (result != -1) {
                return result;
            }
        }
        return result;
    }

    private static long doParseJobIdFromUrl(String patternString, String url) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }

        return -1;
    }

    /**
     * -job D:\project\DataX\stream2stream.json -jobid 1
     *
     * @param args
     * @throws Exception 关于 DataX 在 window 本地调试问题
     *                   1. 如果在 Linux 部署 DataX 那么启动一个 Job 时, 都会读取 DataX 安装目录下的 plugin 目录
     *                   所以在本地调试需要在 Idea 运行 VM 参数在添加 -Ddatax.home=D:\app\datax 来读取 plugin 目录
     *                   2. 指定运行配置文件以及 JobId 比如 -job D:\project\DataX\stream2stream.json -jobid 1
     */
    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        try {
            // 往下追
            Engine.entry(args);
        } catch (Throwable e) {
            exitCode = 1;
            LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(e));

            if (e instanceof DataXException) {
                DataXException tempException = (DataXException) e;
                ErrorCode errorCode = tempException.getErrorCode();
                if (errorCode instanceof FrameworkErrorCode) {
                    FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
                    exitCode = tempErrorCode.toExitValue();
                }
            }

            System.exit(exitCode);
        }
        System.exit(exitCode);
    }

}
