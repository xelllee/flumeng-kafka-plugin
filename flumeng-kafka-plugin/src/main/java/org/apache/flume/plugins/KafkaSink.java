/*
 *  Copyright (c) 2013.09.06 BeyondJ2EE.
 *  * All right reserved.
 *  * http://beyondj2ee.github.com
 *  * This software is the confidential and proprietary information of BeyondJ2EE
 *  * , Inc. You shall not disclose such Confidential Information and
 *  * shall use it only in accordance with the terms of the license agreement
 *  * you entered into with BeyondJ2EE.
 *  *
 *  * Revision History
 *  * Author              Date                  Description
 *  * ===============    ================       ======================================
 *  *  beyondj2ee
 *
 */

package org.apache.flume.plugins;

/**
 * KAFKA Flume Sink (Kafka 0.8 Beta, Flume 1.4).
 * User: beyondj2ee
 * Date: 13. 9. 4
 * Time: PM 4:32
 */

import java.io.UnsupportedEncodingException;
import java.util.*;

import com.google.gson.Gson;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;


/**
 * kafka sink.
 */
public class KafkaSink extends AbstractSink implements Configurable {
    // - [ constant fields ] ----------------------------------------

    /**
     * The constant logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

    // - [ variable fields ] ----------------------------------------
    /**
     * The Parameters.
     */
    private Properties parameters;
    /**
     * The Producer.
     */
    private Producer<String, String> producer;
    /**
     * The Context.
     */
    private Context context;

    // - [ interface methods ] ------------------------------------

    private SinkCounter sinkCounter;
    private String partitionKey;
    private String encoding;
    private String topic;

    private boolean formatInJson;
    private boolean logEvent;

    private int batchSize;

    /**
     * Configure void.
     *
     * @param context the context
     */
    @Override
    public void configure(Context context) {
        this.context = context;
        ImmutableMap<String, String> props = context.getParameters();

        parameters = new Properties();
        for (String key : props.keySet()) {
            String value = props.get(key);
            this.parameters.put(key, value);
        }

        sinkCounter = new SinkCounter(this.getName());

        partitionKey = (String) parameters.get(KafkaFlumeConstans.PARTITION_KEY_NAME);
        encoding = StringUtils.defaultIfEmpty((String) this.parameters.get(KafkaFlumeConstans.ENCODING_KEY_NAME), KafkaFlumeConstans.DEFAULT_ENCODING);
        topic = Preconditions.checkNotNull((String) this.parameters.get(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME), "custom.topic.name is required");
        formatInJson = Boolean.parseBoolean(StringUtils.defaultIfEmpty((String) this.parameters.get(KafkaFlumeConstans.CUSTOME_FORMAT_IN_JSON), "false"));
        logEvent = Boolean.parseBoolean(StringUtils.defaultIfEmpty((String) this.parameters.get(KafkaFlumeConstans.LOG_EVENT), "false"));
        batchSize = Integer.valueOf(StringUtils.defaultIfEmpty((String) this.parameters.get(KafkaFlumeConstans.FLUME_BATCH_SIZE), "200"));

    }

    /**
     * Start void.
     */
    @Override
    public synchronized void start() {
        super.start();

        try {
            ProducerConfig config = new ProducerConfig(this.parameters);
            this.producer = new Producer<String, String>(config);
            sinkCounter.incrementConnectionCreatedCount();
            sinkCounter.start();
        } catch (Exception e) {
            sinkCounter.incrementConnectionFailedCount();
        }
    }

    /**
     * Process status.
     *
     * @return the status
     * @throws EventDeliveryException the event delivery exception
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {


            List<KeyedMessage<String, String>> msgList = new ArrayList<KeyedMessage<String, String>>();

            int i = 0;
            for (; i < batchSize; i++) {
                // This try clause includes whatever Channel operations you want to do
                Event event = ch.take();

                if (event == null) {
                    status = Status.BACKOFF;
                    if (i == 0) {
                        sinkCounter.incrementBatchEmptyCount();
                    } else {
                        sinkCounter.incrementBatchUnderflowCount();
                    }
                    break;
                } else {
                    KeyedMessage<String, String> data = formatEvent(event);
                    msgList.add(data);
                }
            }

            if (i == batchSize) {
                sinkCounter.incrementBatchCompleteCount();
            }
            sinkCounter.addToEventDrainAttemptCount(i);
            produceAndCommit(msgList, txn);


        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    /**
     * Stop void.
     */
    @Override
    public void stop() {
        try {
            producer.close();
            sinkCounter.incrementConnectionClosedCount();
            sinkCounter.stop();
        } catch (Exception e) {
            sinkCounter.incrementConnectionFailedCount();
        }


    }

    private KeyedMessage<String, String> formatEvent(Event event) throws UnsupportedEncodingException {
        String eventData;
        KeyedMessage<String, String> data;

        String body = new String(event.getBody(), encoding);

        if (formatInJson) {
            Gson gson = new Gson();

            Map<String, String> headers = event.getHeaders();
            Map<String, String> event_map = new HashMap<String, String>();

            event_map.put("body", body);

            for (Map.Entry<String, String> entry : headers.entrySet()) {
                event_map.put(entry.getKey(), entry.getValue());
            }
            eventData = gson.toJson(event_map);
        } else {
            eventData = body;
        }

        // if partition key does'nt exist
        if (StringUtils.isEmpty(partitionKey)) {
            data = new KeyedMessage<String, String>(topic, eventData);
        } else {
            data = new KeyedMessage<String, String>(topic, partitionKey, eventData);
        }

        if (logEvent) {
            LOGGER.info("Send Message to Kafka : [" + eventData + "] -- [" + EventHelper.dumpEvent(event) + "]");
        }
        return data;
    }

    private void produceAndCommit(final List<KeyedMessage<String, String>> msgList, Transaction txn) {

        for (KeyedMessage<String, String> msg : msgList) {
            producer.send(msg);
        }
        txn.commit();
        sinkCounter.addToEventDrainSuccessCount(msgList.size());

    }

}
