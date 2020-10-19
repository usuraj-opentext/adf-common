package com.opentext.adf.common.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;
import java.util.UUID;

/**
 * Utility classes for the Kafka
 */
//TODO
public class AdfKafkaClient {
    public Consumer getKafkaConsumer(Map<String, Object> config) {
        config.computeIfPresent(ProducerConfig.CLIENT_ID_CONFIG, (k, v) -> v + "_" + UUID.randomUUID().toString());
        return new KafkaConsumer<>(config);
    }

    public Producer getKafkaProducer(Map<String, Object> config) {
        config.computeIfPresent(ConsumerConfig.CLIENT_ID_CONFIG, (k, v) -> v + "_" + UUID.randomUUID().toString());
        return new KafkaProducer<>(config);
    }
}
