/**
 * Copyright 2016 Evokly S.A.
 * See LICENSE file for License
 **/

package com.andresgomezfrr.kafka.connectors.mqtt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MqttSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(MqttSourceConnector.class);

    MqttSourceConnectorConfig mConfig;
    private Map<String, String> mConfigProperties;


    @Override
    public String version() {
        return "0.0.1";
    }


    @Override
    public void start(Map<String, String> props) {
        log.info("Start a MqttSourceConnector");
        mConfigProperties = props;
        mConfig = new MqttSourceConnectorConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(1);
        Map<String, String> taskProps = new HashMap<>(mConfigProperties);
        taskConfigs.add(taskProps);
        return taskConfigs;
    }


    @Override
    public void stop() {
        log.info("Stop the MqttSourceConnector");
    }

    @Override
    public ConfigDef config() {
        return MqttSourceConnectorConfig.config;
    }
}
