package com.alibaba.datax.core.transport.channel.memory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存Channel的具体实现，底层其实是一个ArrayBlockingQueue
 *
 */
public class MemoryChannel extends Channel {

	// 默认值为 32
	private int bufferSize = 0;

	private AtomicInteger memoryBytes = new AtomicInteger(0);

	private ArrayBlockingQueue<Record> queue = null;

	private ReentrantLock lock;

	private Condition notInsufficient, notEmpty;

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
	 * 			"taskId": 1,
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
	public MemoryChannel(final Configuration configuration) {
		// 调用父类
		super(configuration);
		// 默认容量 2048
		this.queue = new ArrayBlockingQueue<Record>(this.getCapacity());
		// 默认 32
		this.bufferSize = configuration.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);

		// 相关锁初始化
		lock = new ReentrantLock();
		notInsufficient = lock.newCondition();
		notEmpty = lock.newCondition();
	}

	@Override
	public void close() {
		super.close();
		try {
			this.queue.put(TerminateRecord.get());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void clear(){
		this.queue.clear();
	}

	@Override
	protected void doPush(Record r) {
		try {
			long startTime = System.nanoTime();
			// 添加 TerminateRecord
			this.queue.put(r);
			waitWriterTime += System.nanoTime() - startTime;
            memoryBytes.addAndGet(r.getMemorySize());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void doPushAll(Collection<Record> rs) {
		try {
			long startTime = System.nanoTime();
			lock.lockInterruptibly();
			// 获取当前 rs 字节大小
			int bytes = getRecordBytes(rs);

			while (
					// 当前 memoryBytes + 当前 rs 字节大小 > 8M
					memoryBytes.get() + bytes > this.byteCapacity
					||
							// 当前 rs 的大小 大于阻塞队列的可用大小
							rs.size() > this.queue.remainingCapacity()) {
				notInsufficient.await(200L, TimeUnit.MILLISECONDS);
            }

			// 添加数据 queue
			this.queue.addAll(rs);

			waitWriterTime += System.nanoTime() - startTime;

			// 累计字节数
			memoryBytes.addAndGet(bytes);

			// 唤醒 notEmpty 因为有数据进来
			notEmpty.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	protected Record doPull() {
		try {
			long startTime = System.nanoTime();
			Record r = this.queue.take();
			waitReaderTime += System.nanoTime() - startTime;
			memoryBytes.addAndGet(-r.getMemorySize());
			return r;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected void doPullAll(Collection<Record> rs) {
		assert rs != null;
		rs.clear();
		try {
			long startTime = System.nanoTime();
			lock.lockInterruptibly();
			// 拉取 queue 队列数据到 rs 返回拉取数据记录数
			// 只有拉取到数据 this.queue.drainTo(rs, bufferSize) 才会返回 > 0
			// 所以如果拉取不到数据只能等待 直到拉取数据
			while (this.queue.drainTo(rs, bufferSize) <= 0) {
				// 如果拉取不到数据只能等待 200 ms
				notEmpty.await(200L, TimeUnit.MILLISECONDS);
			}
			waitReaderTime += System.nanoTime() - startTime;
			int bytes = getRecordBytes(rs);

			// 减少
			memoryBytes.addAndGet(-bytes);
			// 唤醒
			notInsufficient.signalAll();
		} catch (InterruptedException e) {
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		} finally {
			lock.unlock();
		}
	}

	private int getRecordBytes(Collection<Record> rs){
		int bytes = 0;
		for(Record r : rs){
			bytes += r.getMemorySize();
		}
		return bytes;
	}

	@Override
	public int size() {
		return this.queue.size();
	}

	@Override
	public boolean isEmpty() {
		return this.queue.isEmpty();
	}

}
