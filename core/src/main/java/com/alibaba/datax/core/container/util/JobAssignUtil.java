package com.alibaba.datax.core.container.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public final class JobAssignUtil {
    private JobAssignUtil() {
    }

    /**
     * 公平的分配 task 到对应的 taskGroup 中。
     * 公平体现在：会考虑 task 中对资源负载作的 load 标识进行更均衡的作业分配操作。
     */
    public static List<Configuration> assignFairly(
            Configuration configuration,
            int channelNumber,
            int channelsPerTaskGroup) {
        Validate.isTrue(configuration != null, "框架获得的 Job 不能为 null.");

        // 获取任务配置 默认为 5个配置
        List<Configuration> contentConfig = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        Validate.isTrue(contentConfig.size() > 0, "框架获得的切分后的 Job 无内容.");

        Validate.isTrue(channelNumber > 0 && channelsPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");

        // 计算 TaskGroup 个数 1 = 5 / 5
        int taskGroupNumber = (int) Math.ceil(1.0 * channelNumber / channelsPerTaskGroup);

        Configuration aTaskConfig = contentConfig.get(0);

        // 默认为 null
        String readerResourceMark = aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." +
                CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
        // 默认为 null
        String writerResourceMark = aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." +
                CommonConstant.LOAD_BALANCE_RESOURCE_MARK);

        boolean hasLoadBalanceResourceMark = StringUtils.isNotBlank(readerResourceMark) ||
                StringUtils.isNotBlank(writerResourceMark);

        // 设置
        if (!hasLoadBalanceResourceMark) {
            // fake 一个固定的 key 作为资源标识（在 reader 或者 writer 上均可，此处选择在 reader 上进行 fake）
            for (Configuration conf : contentConfig) {
                conf.set(CoreConstant.JOB_READER_PARAMETER + "." +
                        CommonConstant.LOAD_BALANCE_RESOURCE_MARK, "aFakeResourceMarkForLoadBalance");
            }
            // 是为了避免某些插件没有设置 资源标识 而进行了一次随机打乱操作
            Collections.shuffle(contentConfig, new Random(System.currentTimeMillis()));
        }

        // 主要获取 key = resourceMark value = [taskId ... ]
        LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap =
                parseAndGetResourceMarkAndTaskIdMap(contentConfig);

        // 分配
        /*
        {
	"common": {
		"column": {
			"dateFormat": "yyyy-MM-dd",
			"datetimeFormat": "yyyy-MM-dd HH:mm:ss",
			"encoding": "utf-8",
			"extraFormats": ["yyyyMMdd"],
			"timeFormat": "HH:mm:ss",
			"timeZone": "GMT+8"
		}
	},
	"core": {
		"container": {
			"job": {
				"id": 1,
				"reportInterval": 10000
			},
			"taskGroup": {
				"channel": 5,
				"id": 0
			},
			"trace": {
				"enable": "false"
			}
		},
		"dataXServer": {
			"address": "http://localhost:7001/api",
			"reportDataxLog": false,
			"reportPerfLog": false,
			"timeout": 10000
		},
		"statistics": {
			"collector": {
				"plugin": {
					"maxDirtyNumber": 10,
					"taskClass": "com.alibaba.datax.core.statistics.plugin.task.StdoutPluginCollector"
				}
			}
		},
		"transport": {
			"channel": {
				"byteCapacity": 67108864,
				"capacity": 512,
				"class": "com.alibaba.datax.core.transport.channel.memory.MemoryChannel",
				"flowControlInterval": 20,
				"speed": {
					"byte": -1,
					"record": -1
				}
			},
			"exchanger": {
				"bufferSize": 32,
				"class": "com.alibaba.datax.core.plugin.BufferedRecordExchanger"
			}
		}
	},
	"entry": {
		"jvm": "-Xms1G -Xmx1G"
	},
	"job": {
		"content": [{
			"reader": {
				"name": "streamreader",
				"parameter": {
					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
					"sliceRecordCount": 10
				}
			},
			"taskId": 0,
			"writer": {
				"name": "streamwriter",
				"parameter": {
					"encoding": "UTF-8",
					"print": true
				}
			}
		}, {
			"reader": {
				"name": "streamreader",
				"parameter": {
					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
					"sliceRecordCount": 10
				}
			},
			"taskId": 3,
			"writer": {
				"name": "streamwriter",
				"parameter": {
					"encoding": "UTF-8",
					"print": true
				}
			}
		}, {
			"reader": {
				"name": "streamreader",
				"parameter": {
					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
					"sliceRecordCount": 10
				}
			},
			"taskId": 1,
			"writer": {
				"name": "streamwriter",
				"parameter": {
					"encoding": "UTF-8",
					"print": true
				}
			}
		}, {
			"reader": {
				"name": "streamreader",
				"parameter": {
					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
					"sliceRecordCount": 10
				}
			},
			"taskId": 4,
			"writer": {
				"name": "streamwriter",
				"parameter": {
					"encoding": "UTF-8",
					"print": true
				}
			}
		}, {
			"reader": {
				"name": "streamreader",
				"parameter": {
					"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
					"sliceRecordCount": 10
				}
			},
			"taskId": 2,
			"writer": {
				"name": "streamwriter",
				"parameter": {
					"encoding": "UTF-8",
					"print": true
				}
			}
		}],
		"setting": {
			"speed": {
				"channel": 5
			}
		}
	},
	"plugin": {
		"reader": {
			"streamreader": {
				"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
				"description": {
					"mechanism": "use datax framework to transport data from stream.",
					"useScene": "only for developer test.",
					"warn": "Never use it in your real job."
				},
				"developer": "alibaba",
				"name": "streamreader",
				"path": "D:\\app\\datax\\plugin\\reader\\streamreader"
			}
		},
		"writer": {
			"streamwriter": {
				"class": "com.alibaba.datax.plugin.writer.streamwriter.StreamWriter",
				"description": {
					"mechanism": "use datax framework to transport data to stream.",
					"useScene": "only for developer test.",
					"warn": "Never use it in your real job."
				},
				"developer": "alibaba",
				"name": "streamwriter",
				"path": "D:\\app\\datax\\plugin\\writer\\streamwriter"
			}
		}
	}
}
         */
        // 切分好的 task 需要进行配置到 TaskGroup
        List<Configuration> taskGroupConfig = doAssign(
                resourceMarkAndTaskIdMap,
                configuration,
                taskGroupNumber // Task Group 个数
        );

        // 调整 每个 taskGroup 对应的 Channel 个数（属于优化范畴）
        adjustChannelNumPerTaskGroup(taskGroupConfig, channelNumber);
        return taskGroupConfig;
    }

    private static void adjustChannelNumPerTaskGroup(List<Configuration> taskGroupConfig, int channelNumber) {
        // 默认 1
        int taskGroupNumber = taskGroupConfig.size();
        // 5 = 5 / 1
        int avgChannelsPerTaskGroup = channelNumber / taskGroupNumber;
        // 0 = 5 % 1
        int remainderChannelCount = channelNumber % taskGroupNumber;
        // 表示有 remainderChannelCount 个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup + 1；
        // （taskGroupNumber - remainderChannelCount）个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup

        int i = 0;
        for (; i < remainderChannelCount; i++) {
            taskGroupConfig.get(i).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup + 1);
        }

        for (int j = 0; j < taskGroupNumber - remainderChannelCount; j++) {
            taskGroupConfig.get(i + j).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup);
        }
    }

    /**
     * 根据task 配置，获取到：
     * 资源名称 --> taskId(List) 的 map 映射关系
     */
    private static LinkedHashMap<String, List<Integer>> parseAndGetResourceMarkAndTaskIdMap(List<Configuration> contentConfig) {
        // key: resourceMark, value: taskId
        LinkedHashMap<String, List<Integer>> readerResourceMarkAndTaskIdMap = new LinkedHashMap<String, List<Integer>>();
        LinkedHashMap<String, List<Integer>> writerResourceMarkAndTaskIdMap = new LinkedHashMap<String, List<Integer>>();

        for (Configuration aTaskConfig : contentConfig) {
            int taskId = aTaskConfig.getInt(CoreConstant.TASK_ID);
            // 把 readerResourceMark 加到 readerResourceMarkAndTaskIdMap 中
            String readerResourceMark = aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            if (readerResourceMarkAndTaskIdMap.get(readerResourceMark) == null) {
                readerResourceMarkAndTaskIdMap.put(readerResourceMark, new LinkedList<Integer>());
            }
            readerResourceMarkAndTaskIdMap.get(readerResourceMark).add(taskId);

            // 把 writerResourceMark 加到 writerResourceMarkAndTaskIdMap 中
            String writerResourceMark = aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            if (writerResourceMarkAndTaskIdMap.get(writerResourceMark) == null) {
                writerResourceMarkAndTaskIdMap.put(writerResourceMark, new LinkedList<Integer>());
            }
            writerResourceMarkAndTaskIdMap.get(writerResourceMark).add(taskId);
        }

        if (readerResourceMarkAndTaskIdMap.size() >= writerResourceMarkAndTaskIdMap.size()) {
            // 采用 reader 对资源做的标记进行 shuffle
            return readerResourceMarkAndTaskIdMap;
        } else {
            // 采用 writer 对资源做的标记进行 shuffle
            return writerResourceMarkAndTaskIdMap;
        }
    }

    /**
     * /**
     * 需要实现的效果通过例子来说是：
     * <pre>
     * a 库上有表：0, 1, 2
     * a 库上有表：3, 4
     * c 库上有表：5, 6, 7
     *
     * 如果有 4个 taskGroup
     * 则 assign 后的结果为：
     * taskGroup-0: 0,  4,
     * taskGroup-1: 3,  6,
     * taskGroup-2: 5,  2,
     * taskGroup-3: 1,  7
     *
     * </pre>
     */
    private static List<Configuration> doAssign(LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap,
                                                Configuration jobConfiguration,
                                                int taskGroupNumber) {
        // 基于场景驱动 5 个配置文件参数
        List<Configuration> contentConfig = jobConfiguration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

        // jobConfiguration 表示全部的配置信息
        Configuration taskGroupTemplate = jobConfiguration.clone();

        // 移除 job.content 的所有内容 也就是任务运行参数 比如 contentConfig
        taskGroupTemplate.remove(CoreConstant.DATAX_JOB_CONTENT);

        List<Configuration> result = new LinkedList<Configuration>();

        // taskGroupNumber = 1
        List<List<Configuration>> taskGroupConfigList = new ArrayList<List<Configuration>>(taskGroupNumber);
        for (int i = 0; i < taskGroupNumber; i++) {
            taskGroupConfigList.add(new LinkedList<Configuration>());
        }

        int mapValueMaxLength = -1;

        // 基于场景驱动 resourceMarks 只有一个元素 aFakeResourceMarkForLoadBalance
        List<String> resourceMarks = new ArrayList<String>();
        for (Map.Entry<String, List<Integer>> entry : resourceMarkAndTaskIdMap.entrySet()) {
            resourceMarks.add(entry.getKey());
            if (entry.getValue().size() > mapValueMaxLength) {
                // 赋值为 5
                mapValueMaxLength = entry.getValue().size();
            }
        }

        int taskGroupIndex = 0;
        for (int i = 0; i < mapValueMaxLength; i++) {
            for (String resourceMark : resourceMarks) {
                if (resourceMarkAndTaskIdMap.get(resourceMark).size() > 0) {
                    int taskId = resourceMarkAndTaskIdMap.get(resourceMark).get(0);
                    // 轮询方式
                    taskGroupConfigList.get(taskGroupIndex % taskGroupNumber).add(contentConfig.get(taskId));
                    taskGroupIndex++;

                    resourceMarkAndTaskIdMap.get(resourceMark).remove(0);
                }
            }
        }

        Configuration tempTaskGroupConfig;
        for (int i = 0; i < taskGroupNumber; i++) {
            tempTaskGroupConfig = taskGroupTemplate.clone();
            tempTaskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupConfigList.get(i));
            tempTaskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);

            result.add(tempTaskGroupConfig);
        }

        /**
         * 比如  result 其中一个 TaskGroup 真正切好分配好的任务如下 (基于场景驱动 默认存在 1 个如下内容, 因为只有一个 taskGroup)
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
        return result;
    }

}
