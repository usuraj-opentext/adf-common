package com.opentext.adf.common.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Using MapR Kafka version requires producer interceptors. Provide a default No-Op implementation.
 */
public class NoOpProducerInterceptor<K, V> implements org.apache.kafka.clients.producer.ProducerInterceptor<K, V> {
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}