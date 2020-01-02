package com.andresgomezfrr.kafka.connectors.mqtt;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * MqttSourceTask is a Kafka Connect SourceTask implementation that reads
 * from MQTT and generates Kafka Connect records.
 */
public class MqttSourceTask extends SourceTask implements MqttCallback {
    private static final Logger log = LoggerFactory.getLogger(MqttSourceConnector.class);

    MqttClient mClient;
    String mKafkaTopic;
    String mMqttClientId;
    public BlockingQueue<MqttMessageProcessor> mQueue = new LinkedBlockingQueue<>();
    MqttSourceConnectorConfig mConfig;
    MqttConnectOptions connectOptions;


    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Start a MqttSourceTask");

        mConfig = new MqttSourceConnectorConfig(props);


        mMqttClientId = mConfig.getString(MqttSourceConstant.MQTT_CLIENT_ID) != null
                ? mConfig.getString(MqttSourceConstant.MQTT_CLIENT_ID)
                : MqttClient.generateClientId();

        // Setup Kafka
        mKafkaTopic = mConfig.getString(MqttSourceConstant.KAFKA_TOPIC);


        // Setup MQTT Connect Options
        connectOptions = new MqttConnectOptions();


        if (mConfig.getBoolean(MqttSourceConstant.MQTT_CLEAN_SESSION)) {
            connectOptions.setCleanSession(
                    mConfig.getBoolean(MqttSourceConstant.MQTT_CLEAN_SESSION));
        }
        connectOptions.setConnectionTimeout(
                mConfig.getInt(MqttSourceConstant.MQTT_CONNECTION_TIMEOUT));
        connectOptions.setKeepAliveInterval(
                mConfig.getInt(MqttSourceConstant.MQTT_KEEP_ALIVE_INTERVAL));
        connectOptions.setServerURIs(
                mConfig.getString(MqttSourceConstant.MQTT_SERVER_URIS).split(","));

        if (mConfig.getString(MqttSourceConstant.MQTT_USERNAME) != null) {
            connectOptions.setUserName(
                    mConfig.getString(MqttSourceConstant.MQTT_USERNAME));
        }

        if (mConfig.getString(MqttSourceConstant.MQTT_PASSWORD) != null) {
            connectOptions.setPassword(
                    mConfig.getString(MqttSourceConstant.MQTT_PASSWORD).toCharArray());
        }

        initConnection();
    }

    public void initConnection() {
        // Connect to Broker

        boolean connected = false;
        int retries = 0;

        while (!connected && retries < mConfig.getInt(MqttSourceConstant.MQTT_CONNECTION_RETRIES)) {
            try {
                // Address of the server to connect to, specified as a URI, is overridden using
                // MqttConnectOptions#setServerURIs(String[]) bellow.
                mClient = new MqttClient("tcp://127.0.0.1:1883", mMqttClientId,
                        new MemoryPersistence());
                mClient.setCallback(this);
                mClient.connect(connectOptions);

                log.info("[{}] Connected to Broker", mMqttClientId);
                connected = true;
            } catch (MqttException e) {
                log.error("[{}] Connection to Broker failed!", mMqttClientId, e);
                connected = false;
                retries++;
                try {
                    log.info("Trying to reconnect try #{}/" +
                            mConfig.getInt(MqttSourceConstant.MQTT_CONNECTION_RETRIES) +
                            " waiting(5 sec)", retries, e);
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        String topic = mConfig.getString(MqttSourceConstant.MQTT_TOPIC);
        Integer qos = mConfig.getInt(MqttSourceConstant.MQTT_QUALITY_OF_SERVICE);


        try {
            mClient.unsubscribe(topic);
            log.info("[{}] Unsubscribe to '{}' ", mMqttClientId, topic);
        } catch (MqttException e) {
            log.error("[{}] Unsubscribe failed! ", mMqttClientId, e);
        }

        try {
            mClient.subscribe(topic, qos);
            log.info("[{}] Subscribe to '{}' with QoS '{}'", mMqttClientId, topic,
                    qos.toString());
        } catch (MqttException e) {
            log.error("[{}] Subscribe failed! ", mMqttClientId, e);
        }
    }

    /**
     * Stop this task.
     */
    @Override
    public void stop() {
        log.info("Stoping the MqttSourceTask");

        try {
            mClient.disconnect(1000);

            log.info("[{}] Disconnected from Broker.", mMqttClientId);
        } catch (MqttException e) {
            log.error("[{}] Disconnecting from Broker failed!", mMqttClientId, e);
        }
    }


    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        MqttMessageProcessor message = mQueue.take();
        log.debug("[{}] Polling new data from queue for '{}' topic.",
                mMqttClientId, mKafkaTopic);

        Collections.addAll(records, message.getRecords(mKafkaTopic));

        return records;
    }


    @Override
    public void connectionLost(Throwable cause) {
        log.error("MQTT connection lost!", cause);
        stop();
        initConnection();
    }


    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Nothing to implement.
    }


    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        log.debug("[{}] New message on '{}' arrived.", mMqttClientId, topic);

        this.mQueue.add(
                mConfig.getConfiguredInstance(MqttSourceConstant.MESSAGE_PROCESSOR,
                        MqttMessageProcessor.class)
                        .process(topic, message)
        );
    }
}
