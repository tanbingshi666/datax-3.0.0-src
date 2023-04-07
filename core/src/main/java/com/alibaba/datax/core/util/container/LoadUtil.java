package com.alibaba.datax.core.util.container;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * 插件加载器，大体上分reader、transformer（还未实现）和writer三中插件类型，
 * reader和writer在执行时又可能出现Job和Task两种运行时（加载的类不同）
 */
public class LoadUtil {
    private static final String pluginTypeNameFormat = "plugin.%s.%s";

    private LoadUtil() {
    }

    private enum ContainerType {
        Job("Job"), Task("Task");
        private String type;

        private ContainerType(String type) {
            this.type = type;
        }

        public String value() {
            return type;
        }
    }

    /**
     * 所有插件配置放置在pluginRegisterCenter中，为区别reader、transformer和writer，还能区别
     * 具体pluginName，故使用pluginType.pluginName作为key放置在该map中
     */
    private static Configuration pluginRegisterCenter;

    /**
     * jarLoader的缓冲
     */
    private static Map<String, JarLoader> jarLoaderCenter = new HashMap<String, JarLoader>();

    /**
     * 设置 pluginConfigs，方便后面插件来获取
     * {
     * "common":{
     * "column":{
     * "dateFormat":"yyyy-MM-dd",
     * "datetimeFormat":"yyyy-MM-dd HH:mm:ss",
     * "encoding":"utf-8",
     * "extraFormats":[
     * "yyyyMMdd"
     * ],
     * "timeFormat":"HH:mm:ss",
     * "timeZone":"GMT+8"
     * }
     * },
     * "core":{
     * "container":{
     * "job":{
     * "id":1,
     * "reportInterval":10000
     * },
     * "taskGroup":{
     * "channel":5
     * },
     * "trace":{
     * "enable":"false"
     * }
     * },
     * "dataXServer":{
     * "address":"http://localhost:8080/monitor/job",
     * "reportDataxLog":true,
     * "reportPerfLog":false,
     * "timeout":10000
     * },
     * "redis":{
     * "host":"hj108",
     * "port":6379
     * },
     * "statistics":{
     * "collector":{
     * "plugin":{
     * "maxDirtyNumber":10,
     * "taskClass":"com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector"
     * }
     * }
     * },
     * "transport":{
     * "channel":{
     * "byteCapacity":67108864,
     * "capacity":512,
     * "class":"com.alibaba.datax.core.transport.channel.memory.MemoryChannel",
     * "flowControlInterval":20,
     * "speed":{
     * "byte":-1,
     * "record":-1
     * }
     * },
     * "exchanger":{
     * "bufferSize":32,
     * "class":"com.alibaba.datax.core.plugin.BufferedRecordExchanger"
     * }
     * }
     * },
     * "entry":{
     * "jvm":"-Xms1G -Xmx1G"
     * },
     * "job":{
     * "content":[
     * {
     * "reader":{
     * "name":"streamreader",
     * "parameter":{
     * "column":[
     * {
     * "type":"long",
     * "value":"10"
     * },
     * {
     * "type":"string",
     * "value":"hello，你好，世界-DataX"
     * }
     * ],
     * "sliceRecordCount":10
     * }
     * },
     * "writer":{
     * "name":"streamwriter",
     * "parameter":{
     * "encoding":"UTF-8",
     * "print":true
     * }
     * }
     * }
     * ],
     * "setting":{
     * "speed":{
     * "channel":5
     * }
     * }
     * },
     * "plugin":{
     * "reader":{
     * "streamreader":{
     * "class":"com.alibaba.datax.plugin.reader.streamreader.StreamReader",
     * "description":{
     * "mechanism":"use datax framework to transport data from stream.",
     * "useScene":"only for developer test.",
     * "warn":"Never use it in your real job."
     * },
     * "developer":"alibaba",
     * "name":"streamreader",
     * "path":"D:\\app\\datax\\plugin\\reader\\streamreader"
     * }
     * },
     * "writer":{
     * "streamwriter":{
     * "class":"com.alibaba.datax.plugin.writer.streamwriter.StreamWriter",
     * "description":{
     * "mechanism":"use datax framework to transport data to stream.",
     * "useScene":"only for developer test.",
     * "warn":"Never use it in your real job."
     * },
     * "developer":"alibaba",
     * "name":"streamwriter",
     * "path":"D:\\app\\datax\\plugin\\writer\\streamwriter"
     * }
     * }
     * }
     * }
     */
    public static void bind(Configuration pluginConfigs) {
        // pluginConfigs 就是当前运行任务的所有配置文件信息
        // Configuration pluginRegisterCenter
        pluginRegisterCenter = pluginConfigs;
    }

    private static String generatePluginKey(PluginType pluginType,
                                            String pluginName) {
        return String.format(pluginTypeNameFormat, pluginType.toString(),
                pluginName);
    }

    private static Configuration getPluginConf(PluginType pluginType,
                                               String pluginName) {
        /**
         * 比如
         * {
         * 	"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
         * 	"description": {
         * 		"mechanism": "use datax framework to transport data from stream.",
         * 		"useScene": "only for developer test.",
         * 		"warn": "Never use it in your real job."
         *        },
         * 	"developer": "alibaba",
         * 	"name": "streamreader",
         * 	"path": "D:\\app\\datax\\plugin\\reader\\streamreader"
         * }
         */
        // pluginRegisterCenter 前面被赋值过 (保存了当前 JOB 的配置信息)
        Configuration pluginConf = pluginRegisterCenter
                .getConfiguration(
                        // 生成 key 比如 plugin.reader.streamreader
                        generatePluginKey(pluginType, pluginName));

        if (null == pluginConf) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_INSTALL_ERROR,
                    String.format("DataX不能找到插件[%s]的配置.",
                            pluginName));
        }

        return pluginConf;
    }

    /**
     * 加载JobPlugin，reader、writer都可能要加载
     *
     * @param pluginType
     * @param pluginName
     * @return
     */
    public static AbstractJobPlugin loadJobPlugin(PluginType pluginType,
                                                  String pluginName) {
        // 获取对应的类 比如 com.alibaba.datax.plugin.reader.streamreader.StreamReader$Job
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(
                pluginType, pluginName, ContainerType.Job);

        try {
            // 初始化 比如 com.alibaba.datax.plugin.reader.streamreader.StreamReader 内部静态类 Job
            AbstractJobPlugin jobPlugin = (AbstractJobPlugin) clazz
                    .newInstance();
            // 设置 PluginConf
            // 设置 Job 的 Configuration
            jobPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            // 返回 StreamReader.Job
            return jobPlugin;
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("DataX找到plugin[%s]的Job配置.",
                            pluginName), e);
        }
    }

    /**
     * 加载taskPlugin，reader、writer都可能加载
     *
     * @param pluginType
     * @param pluginName
     * @return
     */
    public static AbstractTaskPlugin loadTaskPlugin(PluginType pluginType,
                                                    String pluginName) {
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(
                pluginType, pluginName, ContainerType.Task);

        try {
            // 第一次进来 class com.alibaba.datax.plugin.writer.streamwriter.StreamWriter$Task
            // 第二次进来 class com.alibaba.datax.plugin.reader.streamreader.StreamReader$Task
            AbstractTaskPlugin taskPlugin = (AbstractTaskPlugin) clazz
                    .newInstance();
            taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return taskPlugin;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("DataX不能找plugin[%s]的Task配置.",
                            pluginName), e);
        }
    }

    /**
     * 根据插件类型、名字和执行时taskGroupId加载对应运行器
     *
     * @param pluginType
     * @param pluginName
     * @return
     */
    public static AbstractRunner loadPluginRunner(PluginType pluginType, String pluginName) {
        // 比如 PluginType = writer pluginName = streamwriter
        AbstractTaskPlugin taskPlugin = LoadUtil.loadTaskPlugin(pluginType,
                pluginName);

        // 根据插件类型创建对应的 Runner
        switch (pluginType) {
            case READER:
                return new ReaderRunner(taskPlugin);
            case WRITER:
                return new WriterRunner(taskPlugin);
            default:
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        String.format("插件[%s]的类型必须是[reader]或[writer]!",
                                pluginName));
        }
    }

    /**
     * 反射出具体plugin实例
     *
     * @param pluginType
     * @param pluginName
     * @param pluginRunType
     * @return
     */
    @SuppressWarnings("unchecked")
    private static synchronized Class<? extends AbstractPlugin> loadPluginClass(
            PluginType pluginType, String pluginName,
            ContainerType pluginRunType) {
        /**
         *  比如
         *  {
         * 	"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
         * 	"description": {
         * 		"mechanism": "use datax framework to transport data from stream.",
         * 		"useScene": "only for developer test.",
         * 		"warn": "Never use it in your real job."
         *        },
         * 	"developer": "alibaba",
         * 	"name": "streamreader",
         * 	"path": "D:\\app\\datax\\plugin\\reader\\streamreader"
         * }
         */
        Configuration pluginConf = getPluginConf(pluginType, pluginName);
        // 获取或者创建 JarLoader
        JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
        try {

            return (Class<? extends AbstractPlugin>) Class.forName(pluginConf.getString("class") + "$"
                    + pluginRunType.value());

            // 反射出具体的 plugin class
//            return (Class<? extends AbstractPlugin>) jarLoader
//                    .loadClass(
//                            // 针对 Reader | Writer
//                            // 比如 第一次进来 com.alibaba.datax.plugin.reader.streamreader.StreamReader$Job
//                            // 比如 第二次进来 com.alibaba.datax.plugin.reader.streamreader.StreamReader$Task
//                            pluginConf.getString("class") + "$" + pluginRunType.value());
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    public static synchronized JarLoader getJarLoader(PluginType pluginType,
                                                      String pluginName) {
        // 获取插件配置
        /**
         * 比如 plugin.reader.streamreader
         * {
         * 	"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
         * 	"description": {
         * 		"mechanism": "use datax framework to transport data from stream.",
         * 		"useScene": "only for developer test.",
         * 		"warn": "Never use it in your real job."
         *        },
         * 	"developer": "alibaba",
         * 	"name": "streamreader",
         * 	"path": "D:\\app\\datax\\plugin\\reader\\streamreader"
         * }
         */
        Configuration pluginConf = getPluginConf(pluginType, pluginName);

        JarLoader jarLoader = jarLoaderCenter.get(
                // 生成对应的 key 比如 plugin.reader.streamreader
                generatePluginKey(pluginType, pluginName));
        if (null == jarLoader) {
            // 获取插件路径 path
            String pluginPath = pluginConf.getString("path");
            if (StringUtils.isBlank(pluginPath)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        String.format(
                                "%s插件[%s]路径非法!",
                                pluginType, pluginName));
            }
            // 根据插件 path 创建 JarLoader
            jarLoader = new JarLoader(new String[]{pluginPath});
            // 根据 path 添加对应的 JarLoader
            // 比如 reader key = plugin.reader.streamreader value = JarLoader(path = D:\\app\\datax\\plugin\\reader\\streamreader)
            // 那么这个 JarLoader 就会加载 path 路径下的所有 jar (递归读取)
            jarLoaderCenter.put(generatePluginKey(pluginType, pluginName),
                    jarLoader);
        }

        // 返回 JarLoader
        return jarLoader;
    }
}
