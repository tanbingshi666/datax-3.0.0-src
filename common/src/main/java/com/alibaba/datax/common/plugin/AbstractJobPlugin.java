package com.alibaba.datax.common.plugin;

/**
 * Created by jingxing on 14-8-24.
 */
public abstract class AbstractJobPlugin extends AbstractPlugin {
    /**
     * @return the jobPluginCollector
     */
    public JobPluginCollector getJobPluginCollector() {
        return jobPluginCollector;
    }

    /**
     * @param jobPluginCollector the jobPluginCollector to set
     */
    public void setJobPluginCollector(
            JobPluginCollector jobPluginCollector) {
        /**
         * 比如 reader
         * jobPluginCollector = DefaultJobPluginCollector(null)
         */
        this.jobPluginCollector = jobPluginCollector;
    }

    private JobPluginCollector jobPluginCollector;

}
