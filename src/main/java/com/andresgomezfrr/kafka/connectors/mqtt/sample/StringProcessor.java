package com.andresgomezfrr.kafka.connectors.mqtt.sample;

import com.andresgomezfrr.kafka.connectors.mqtt.MqttMessageProcessor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

public class StringProcessor implements MqttMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(StringProcessor.class);
    private MqttMessage mMessage;
    private Object mTopic;

    @Override
    public MqttMessageProcessor process(String topic, MqttMessage message) {
        log.debug("processing data for topic: {}; with message {}", topic, message);
        this.mTopic = topic;
        this.mMessage = message;
        return this;
    }

    @Override
    public SourceRecord[] getRecords(String kafkaTopic) {
        SourceRecord[] records;
        try {
            records = new SourceRecord[]{new SourceRecord(null, null, kafkaTopic, null,
                    Schema.STRING_SCHEMA, mTopic,
                    Schema.STRING_SCHEMA, new String(mMessage.getPayload(), "UTF-8"))};
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage(), e);
            records = new SourceRecord[]{};
        }

        return records;
    }
}
