package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.common.util.Configuration;

public abstract class AbstractPlugin extends BaseObject implements Pluginable {
    //作业的config
    private Configuration pluginJobConf;

    //插件本身的plugin
    private Configuration pluginConf;

    // by qiangsi.lq。 修改为对端的作业configuration
    private Configuration peerPluginJobConf;

    private String peerPluginName;

    @Override
    public String getPluginName() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("name");
    }

    @Override
    public String getDeveloper() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("developer");
    }

    @Override
    public String getDescription() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("description");
    }

    @Override
    public Configuration getPluginJobConf() {
        return pluginJobConf;
    }

    @Override
    public void setPluginJobConf(Configuration pluginJobConf) {
        // 插件配置参数
        /**
         * 比如 reader
         * {
         * 	"column": ["{\"type\":\"long\",\"value\":\"10\"}", "{\"type\":\"string\",\"value\":\"hello，你好，世界-DataX\"}"],
         * 	"sliceRecordCount": 10
         * }
         */
        this.pluginJobConf = pluginJobConf;
    }

    @Override
    public void setPluginConf(Configuration pluginConf) {
        /**
         * 比如 reader 那么配置信息为
         *          * 比如
         *          * {
         *          * 	"class": "com.alibaba.datax.plugin.reader.streamreader.StreamReader",
         *          * 	"description": {
         *          * 		"mechanism": "use datax framework to transport data from stream.",
         *          * 		"useScene": "only for developer test.",
         *          * 		"warn": "Never use it in your real job."
         *          *        },
         *          * 	"developer": "alibaba",
         *          * 	"name": "streamreader",
         *          * 	"path": "D:\\app\\datax\\plugin\\reader\\streamreader"
         *          * }
         *          */
        this.pluginConf = pluginConf;
    }

    @Override
    public Configuration getPeerPluginJobConf() {
        return peerPluginJobConf;
    }

    @Override
    public void setPeerPluginJobConf(Configuration peerPluginJobConf) {
        /**
         * 比如 reader 那么 peerPluginJobConf 信息就是 writer 的参数配置
         *
         * {"encoding":"UTF-8","print":true}
         */
        this.peerPluginJobConf = peerPluginJobConf;
    }

    @Override
    public String getPeerPluginName() {
        return peerPluginName;
    }

    @Override
    public void setPeerPluginName(String peerPluginName) {
        this.peerPluginName = peerPluginName;
    }

    public void preCheck() {
    }

    public void prepare() {
    }

    public void post() {
    }

    public void preHandler(Configuration jobConfiguration) {

    }

    public void postHandler(Configuration jobConfiguration) {

    }
}
