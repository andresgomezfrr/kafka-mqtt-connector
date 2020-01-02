package com.andresgomezfrr.kafka.connectors.mqtt;

import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public interface MqttMessageProcessor {

    MqttMessageProcessor process(String topic, MqttMessage message);

    SourceRecord[] getRecords(String kafkaTopic);
}
