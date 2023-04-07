package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferedRecordExchanger implements RecordSender, RecordReceiver {

    private final Channel channel;

    private final Configuration configuration;

    private final List<Record> buffer;

    private int bufferSize;

    protected final int byteCapacity;

    private final AtomicInteger memoryBytes = new AtomicInteger(0);

    private int bufferIndex = 0;

    private static Class<? extends Record> RECORD_CLASS;

    private volatile boolean shutdown = false;

    private final TaskPluginCollector pluginCollector;

    @SuppressWarnings("unchecked")
    public BufferedRecordExchanger(final Channel channel, final TaskPluginCollector pluginCollector) {
        assert null != channel;
        assert null != channel.getConfiguration();

        this.channel = channel;
        this.pluginCollector = pluginCollector;
        this.configuration = channel.getConfiguration();

        // 默认 32
        this.bufferSize = configuration
                .getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);
        this.buffer = new ArrayList<Record>(bufferSize);

        //channel的queue默认大小为8M，原来为64M
        this.byteCapacity = configuration.getInt(
                CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 8 * 1024 * 1024);

        try {
            BufferedRecordExchanger.RECORD_CLASS = ((Class<? extends Record>) Class
                    .forName(configuration.getString(
                            CoreConstant.DATAX_CORE_TRANSPORT_RECORD_CLASS,
                            "com.alibaba.datax.core.transport.record.DefaultRecord")));
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public Record createRecord() {
        try {
            // 默认调用 com.alibaba.datax.core.transport.record.DefaultRecord
            // 创建 DefaultRecord
            return BufferedRecordExchanger.RECORD_CLASS.newInstance();
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public void sendToWriter(Record record) {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }

        Validate.notNull(record, "record不能为空.");

        // 如果一条数据大小超过了 8M 则为脏数据
        if (record.getMemorySize() > this.byteCapacity) {
            this.pluginCollector.collectDirtyRecord(record, new Exception(String.format("单条记录超过大小限制，当前限制为:%s", this.byteCapacity)));
            return;
        }

        // 判断是否满了
        boolean isFull = (
                // bufferIndex 大于等于 bufferSize
                this.bufferIndex >= this.bufferSize
                        ||
                        // 累计内存大小 + 当前传入的记录大小大于 8M
                        this.memoryBytes.get() + record.getMemorySize() > this.byteCapacity);
        if (isFull) {
            // 满足以上两个条件之一 需要刷写到 MemoryChannel
            flush();
        }

        // 将记录添加到 buffer
        this.buffer.add(record);
        // bufferIndex 累加
        this.bufferIndex++;
        // 内存大小累计
        memoryBytes.addAndGet(record.getMemorySize());
    }

    @Override
    public void flush() {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        // 将接收到 StreamReader$Task 发送过来的数据添加到 MemoryChannel
        this.channel.pushAll(this.buffer);

        // 重置变量
        this.buffer.clear();
        this.bufferIndex = 0;
        this.memoryBytes.set(0);
    }

    @Override
    public void terminate() {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        // 刷鞋数据到 MemoryChannel
        flush();
        // MemoryChannel 追加 Reader 插件读取数据完成信息终止 (TerminateRecord) Writer 插件
        this.channel.pushTerminate(TerminateRecord.get());
    }

    @Override
    public Record getFromReader() {
        if (shutdown) {
            throw DataXException.asDataXException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        // 判断是否数据为空 如果数据缓存为空，可能需要拉取数据
        boolean isEmpty = (this.bufferIndex >= this.buffer.size());
        if (isEmpty) {
            // 接受数据
            receive();
        }

        // 获取一条数据
        Record record = this.buffer.get(this.bufferIndex++);
        // 如果接受到终止信息 那么 record = null
        if (record instanceof TerminateRecord) {
            record = null;
        }

        return record;
    }

    @Override
    public void shutdown() {
        shutdown = true;
        try {
            buffer.clear();
            channel.clear();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void receive() {
        // 拉取数据到 buffer
        this.channel.pullAll(this.buffer);
        // 拉取到数据 重新设置 bufferIndex
        this.bufferIndex = 0;
        // 拉取到数据 重新设置 bufferSize
        this.bufferSize = this.buffer.size();
    }
}
