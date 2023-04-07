package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class ConfigParser {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigParser.class);

    /**
     * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
     */
    public static Configuration parse(final String jobPath) {
        // 解析目标 JSON 文件 封装成 Configuration root 属性
        Configuration configuration = ConfigParser.parseJobConfig(jobPath);

        // 合并 core.json 文件到 root 属性
        // 任务配置文件内容 与 conf/core.json 进行合并
        configuration.merge(
                // 解析 conf/core.json 跟解析任务配置文件内容类似
                // 都是将内容 json 字符串封装成 Configuration 的 root 属性 (JSONObject 对象)
                ConfigParser.parseCoreConfig(CoreConstant.DATAX_CONF_PATH),
                false);

        // 获取 reader 名称
        String readerPluginName = configuration.getString(
                // job.content[0].reader.name -> streamreader
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        // 获取 writer 名称
        String writerPluginName = configuration.getString(
                // job.content[0].writer.name -> streamwriter
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);

        // 获取前置 handler 名称
        String preHandlerName = configuration.getString(
                // job.preHandler.pluginName = null
                CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

        // 获取后置 handler 名称
        String postHandlerName = configuration.getString(
                // job.postHandler.pluginName = null
                CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

        // 将 ReadPluginName & WriterPluginName 添加到插件集合
        Set<String> pluginList = new HashSet<String>();
        pluginList.add(readerPluginName);
        pluginList.add(writerPluginName);

        // 判断前置处理器 & 后置处理器是否存在 如果存在添加到插件集合
        if (StringUtils.isNotEmpty(preHandlerName)) {
            pluginList.add(preHandlerName);
        }
        if (StringUtils.isNotEmpty(postHandlerName)) {
            pluginList.add(postHandlerName);
        }

        try {
            // 通过加载 plugin 下的所有插件进行配置目标，合并到 root 对象
            configuration.merge(parsePluginConfig(new ArrayList<String>(pluginList)), false);
        } catch (Exception e) {
            //吞掉异常，保持log干净。这里message足够。
            LOG.warn(String.format("插件[%s,%s]加载失败，1s后重试... Exception:%s ", readerPluginName, writerPluginName, e.getMessage()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                //
            }
            configuration.merge(parsePluginConfig(new ArrayList<String>(pluginList)), false);
        }

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
         * 				"reportInterval": 10000
         * 			},
         * 			"taskGroup": {
         * 				"channel": 5
         * 			},
         * 			"trace": {
         * 				"enable": "false"
         * 			}
         * 		},
         * 		"dataXServer": {
         * 			"address": "http://localhost:7001/api",
         * 			"reportDataxLog": false,
         * 			"reportPerfLog": false,
         * 			"timeout": 10000
         * 		},
         * 		"statistics": {
         * 			"collector": {
         * 				"plugin": {
         * 					"maxDirtyNumber": 10,
         * 					"taskClass": "com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector"
         * 				}
         * 			}
         * 		},
         * 		"transport": {
         * 			"channel": {
         * 				"byteCapacity": 67108864,
         * 				"capacity": 512,
         * 				"class": "com.alibaba.datax.core.transport.channel.memory.MemoryChannel",
         * 				"flowControlInterval": 20,
         * 				"speed": {
         * 					"byte": -1,
         * 					"record": -1
         * 				}
         * 			},
         * 			"exchanger": {
         * 				"bufferSize": 32,
         * 				"class": "com.alibaba.datax.core.plugin.BufferedRecordExchanger"
         * 			}
         * 		}
         * 	},
         * 	"entry": {
         * 		"jvm": "-Xms1G -Xmx1G"
         * 	},
         * 	"job": {
         * 		"content": [{
         * 			"reader": {
         * 				"name": "streamreader",
         * 				"parameter": {
         * 					"column": [{
         * 						"type": "long",
         * 						"value": "10"
         * 					}, {
         * 						"type": "string",
         * 						"value": "hello，你好，世界-DataX"
         * 					}],
         * 					"sliceRecordCount": 10
         * 				}
         * 			},
         * 			"writer": {
         * 				"name": "streamwriter",
         * 				"parameter": {
         * 					"encoding": "UTF-8",
         * 					"print": true
         * 				}
         * 			}
         * 		}],
         * 		"setting": {
         * 			"speed": {
         * 				"channel": 5
         * 			}
         * 		}
         * 	},
         * 	"plugin": {
         * 		"reader": {
         * 			"streamreader": {
         * 				"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
         * 				"description": {
         * 					"mechanism": "use datax framework to transport data from stream.",
         * 					"useScene": "only for developer test.",
         * 					"warn": "Never use it in your real job."
         * 				},
         * 				"developer": "alibaba",
         * 				"name": "streamreader",
         * 				"path": "D:\\app\\datax\\plugin\\reader\\streamreader"
         * 			}
         * 		},
         * 		"writer": {
         * 			"streamwriter": {
         * 				"class": "com.alibaba.datax.plugin.writer.streamwriter.StreamWriter",
         * 				"description": {
         * 					"mechanism": "use datax framework to transport data to stream.",
         * 					"useScene": "only for developer test.",
         * 					"warn": "Never use it in your real job."
         * 				},
         * 				"developer": "alibaba",
         * 				"name": "streamwriter",
         * 				"path": "D:\\app\\datax\\plugin\\writer\\streamwriter"
         * 			}
         * 		}
         * 	}
         * }
         */
        return configuration;
    }

    private static Configuration parseCoreConfig(final String path) {
        return Configuration.from(new File(path));
    }

    /**
     * 解析 D:\project\DataX\stream2stream.json 配置文件
     */
    public static Configuration parseJobConfig(final String path) {
        // 获取配置文件内容
        String jobContent = getJobContent(path);
        // 解析配置文件内容 将配置文件内容封装成 Configuration 对象
        // Configuration 对象的 root 属性本质是 JSONObject 里面内容就是任务配置内容
        Configuration config = Configuration.from(jobContent);

        // 判断是否需要解密 一般情况下都是不需要进行解密的
        return SecretUtil.decryptSecretKey(config);
    }

    /**
     * 获取 Job 配置文件内容
     */
    private static String getJobContent(String jobResource) {
        String jobContent;

        // 判断任务文件路径是否以 http 开头
        boolean isJobResourceFromHttp = jobResource.trim().toLowerCase().startsWith("http");

        // 如果任务配置路径是以 http 开头，可能配置文件在远程，需要拉取数据
        if (isJobResourceFromHttp) {
            //设置httpclient的 HTTP_TIMEOUT_INMILLIONSECONDS
            // 读取 conf/core.json 文件
            Configuration coreConfig = ConfigParser.parseCoreConfig(CoreConstant.DATAX_CONF_PATH);
            int httpTimeOutInMillionSeconds = coreConfig.getInt(
                    CoreConstant.DATAX_CORE_DATAXSERVER_TIMEOUT, 5000);
            HttpClientUtil.setHttpTimeoutInMillionSeconds(httpTimeOutInMillionSeconds);

            HttpClientUtil httpClientUtil = new HttpClientUtil();
            try {
                URL url = new URL(jobResource);
                HttpGet httpGet = HttpClientUtil.getGetRequest();
                httpGet.setURI(url.toURI());

                jobContent = httpClientUtil.executeAndGetWithFailedRetry(httpGet, 1, 1000l);
            } catch (Exception e) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "获取作业配置信息失败:" + jobResource, e);
            }
        } else {
            // jobResource 是本地文件绝对路径
            try {
                // 读取配置文件内容
                jobContent = FileUtils.readFileToString(new File(jobResource));
            } catch (IOException e) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "获取作业配置信息失败:" + jobResource, e);
            }
        }

        if (jobContent == null) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "获取作业配置信息失败:" + jobResource);
        }

        // 返回任务配置文件内容
        return jobContent;
    }

    /**
     * 根据插件名称进行加载 datax/plugin 目录下的插件 jar
     * 基于场景驱动，wantPluginNames = [streamreader,streamwriter]
     */
    public static Configuration parsePluginConfig(List<String> wantPluginNames) {
        Configuration configuration = Configuration.newDefault();

        Set<String> replicaCheckPluginSet = new HashSet<String>();
        int complete = 0;

        // 获取 ${datax.home}/plugin/reader 目录下的所有文件夹 (文件夹下面就是 jar)
        // 比如 D:\app\datax\plugin\reader\cassandrareader
        for (final String each : ConfigParser
                .getDirAsList(CoreConstant.DATAX_PLUGIN_READER_HOME)) {

            // eachReaderConfig 就是从 D:\app\datax\plugin\reader\streamreader\plugin.json 解析出来的 Configuration
            /**
             *{
             * 	"plugin": {
             * 		"reader": {
             * 			"streamreader": {
             * 				"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
             * 				"description": {
             * 					"mechanism": "use datax framework to transport data from stream.",
             * 					"useScene": "only for developer test.",
             * 					"warn": "Never use it in your real job."
             *                                },
             * 				"developer": "alibaba",
             * 				"name": "streamreader",
             * 				"path": "D:\\app\\datax\\plugin\\reader\\streamreader" 			}
             * 		}
             * 	}
             * }
             */
            Configuration eachReaderConfig = ConfigParser.parseOnePluginConfig(each,
                    "reader", replicaCheckPluginSet, wantPluginNames);
            if (eachReaderConfig != null) {
                // 合并
                configuration.merge(eachReaderConfig, true);
                complete += 1;
            }
        }

        // 获取 ${datax.home}/plugin/writer 目录下的所有文件夹 (文件夹下面就是 jar)
        for (final String each : ConfigParser
                .getDirAsList(CoreConstant.DATAX_PLUGIN_WRITER_HOME)) {
            /**
             * {
             * 	"plugin": {
             * 		"writer": {
             * 			"streamwriter": {
             * 				"class": "com.alibaba.datax.plugin.writer.streamwriter.StreamWriter",
             * 				"description": {
             * 					"mechanism": "use datax framework to transport data to stream.",
             * 					"useScene": "only for developer test.",
             * 					"warn": "Never use it in your real job."
             *                                },
             * 				"developer": "alibaba",
             * 				"name": "streamwriter",
             * 				"path": "D:\\app\\datax\\plugin\\writer\\streamwriter"* 			}
             * 		}
             * 	}
             * }
             */
            Configuration eachWriterConfig = ConfigParser.parseOnePluginConfig(each, "writer", replicaCheckPluginSet, wantPluginNames);
            if (eachWriterConfig != null) {
                configuration.merge(eachWriterConfig, true);
                complete += 1;
            }
        }

        if (wantPluginNames != null && wantPluginNames.size() > 0 && wantPluginNames.size() != complete) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_INIT_ERROR, "插件加载失败，未完成指定插件加载:" + wantPluginNames);
        }

        // 返回
        /**
         * {
         * 	"plugin": {
         * 		"reader": {
         * 			"streamreader": {
         * 				"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
         * 				"description": {
         * 					"mechanism": "use datax framework to transport data from stream.",
         * 					"useScene": "only for developer test.",
         * 					"warn": "Never use it in your real job."
         *                                },
         * 				"developer": "alibaba",
         * 				"name": "streamreader",
         * 				"path": "D:\\app\\datax\\plugin\\reader\\streamreader"* 			}
         * 		}        ,
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
        return configuration;
    }

    /**
     * 解析 ${datax.home}/plugin/[reader|writer]/plugin.json 文件 判断该 json 文件的 name 属性的值是否等于任务 reader|writer 对应的名称
     */
    public static Configuration parseOnePluginConfig(final String path,
                                                     final String type,
                                                     Set<String> pluginSet,
                                                     List<String> wantPluginNames) {
        // ${datax.home}/plugin/[reader|writer]/plugin.json
        String filePath = path + File.separator + "plugin.json";
        /**
         * 比如 D:\app\datax\plugin\reader\streamreader/plugin.json 的内容
         *
         * {
         *     "name": "streamreader",
         *     "class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
         *     "description": {
         *         "useScene": "only for developer test.",
         *         "mechanism": "use datax framework to transport data from stream.",
         *         "warn": "Never use it in your real job."
         *     },
         *     "developer": "alibaba"
         * }
         */
        Configuration configuration = Configuration.from(new File(filePath));

        // 获取 path 值 (基于场景驱动 为 null)
        String pluginPath = configuration.getString("path");
        // 获取 name 值 (基于场景驱动 为 streamreader | streamwriter)
        String pluginName = configuration.getString("name");

        // pluginSet 初始化为 empty 也即里面还没有内容
        if (!pluginSet.contains(pluginName)) {
            // add
            pluginSet.add(pluginName);
        } else {
            // 如果存在重复的 plugin 名称 则报错
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_INIT_ERROR, "插件加载失败,存在重复插件:" + filePath);
        }

        //不是想要的插件，返回 null
        if (wantPluginNames != null && wantPluginNames.size() > 0 && !wantPluginNames.contains(pluginName)) {
            return null;
        }

        // 执行到这里说明找到了任务所需要执行的 pluginName

        // 判断 plugin.json 的 path 对应的值是否为 null (基于场景驱动 为 null)
        boolean isDefaultPath = StringUtils.isBlank(pluginPath);
        if (isDefaultPath) {
            // 设置 path 比如 D:\app\datax\plugin\reader\streamreader
            configuration.set("path", path);
        }

        Configuration result = Configuration.newDefault();

        result.set(
                // 设置 key = plugin.[reader|writer].[pluginName]
                // 比如 plugin.reader.streamreader
                String.format("plugin.%s.%s", type, pluginName),
                configuration.getInternal());

        return result;
    }

    private static List<String> getDirAsList(String path) {
        List<String> result = new ArrayList<String>();

        String[] paths = new File(path).list();
        if (null == paths) {
            return result;
        }

        for (final String each : paths) {
            result.add(path + File.separator + each);
        }

        return result;
    }

}
