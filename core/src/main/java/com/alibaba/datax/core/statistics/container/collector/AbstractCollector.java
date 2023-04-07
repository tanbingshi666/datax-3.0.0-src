package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractCollector {
    // TaskId 对应 Communication
    private Map<Integer, Communication> taskCommunicationMap = new ConcurrentHashMap<Integer, Communication>();
    private Long jobId;

    public Map<Integer, Communication> getTaskCommunicationMap() {
        return taskCommunicationMap;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {
        for (Configuration config : taskGroupConfigurationList) {
            // 0
            int taskGroupId = config.getInt(
                    CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            // 注册 TaskGroup 对应的 Communication
            // 当前 job 对应一个 Communication
            LocalTGCommunicationManager.registerTaskGroupCommunication(taskGroupId, new Communication());
        }
    }

    public void registerTaskCommunication(List<Configuration> taskConfigurationList) {
        for (Configuration taskConfig : taskConfigurationList) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            // key = TaskId value = Communication
            // 基于场景驱动 key = 0 1 2 3 4
            this.taskCommunicationMap.put(taskId,
                    // 创建 Communication 进行初始化 TaskId 对应的 task
                    new Communication());
        }
    }

    public Communication collectFromTask() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication :
                this.taskCommunicationMap.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
    }

    public abstract Communication collectFromTaskGroup();

    public Map<Integer, Communication> getTGCommunicationMap() {
        return LocalTGCommunicationManager.getTaskGroupCommunicationMap();
    }

    public Communication getTGCommunication(Integer taskGroupId) {
        return LocalTGCommunicationManager.getTaskGroupCommunication(taskGroupId);
    }

    public Communication getTaskCommunication(Integer taskId) {
        return this.taskCommunicationMap.get(taskId);
    }
}
