/**
 * Copyright 2017 Liaison Technologies, Inc.
 * This software is the confidential and proprietary information of
 * Liaison Technologies, Inc. ("Confidential Information").  You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Liaison Technologies.
 */
package com.opentext.adf.common.kafka;

import com.opentext.adf.common.exception.AdfException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for the kafka admin client
 *
 * @author opentext
 */
//TODO
public class AdfKafkaAdminClient {
    private final Logger LOG = LoggerFactory.getLogger(AdfKafkaAdminClient.class);

    private short replicationFactor = 1;
    private Map<String, Object> config;

    public AdfKafkaAdminClient(Map<String, Object> config) {
        this.config = config;
        Optional.ofNullable(config.get("replication.factor")).ifPresent(value ->
                this.replicationFactor = Short.parseShort(value.toString())
        );
    }

    public void close(AdminClient adminClient) {
        try {
            if (adminClient != null) {
                adminClient.close();
            }
        } catch (Exception ex) {
            LOG.error("Unable to close the kafka admin client - {}", ex.getMessage());
        }
    }

    public void createTopic(String topic, int partition) {
        try {
            createTopic(topic, partition, this.replicationFactor);
        } catch (Exception ex) {
            throw new AdfException("Unable to create topic - " + topic, ex);
        }
    }

    public void createTopic(String topic, int partition, Properties properties) {
        try {
            short replicationFactor = (short) 1;
            if (properties.get("replication.factor") != null) {
                replicationFactor = Short.parseShort(properties.get("replication.factor").toString());
            }
            createTopic(topic, partition, replicationFactor);
        } catch (Exception ex) {
            throw new AdfException("Unable to create topic - " + topic, ex);
        }
    }

    public void createTopic(String topic, int partition, short replicationFactor) {
        AdminClient adminClient = null;
        try {
            adminClient = KafkaAdminClient.create(config);
            CreateTopicsResult topicsResult =
                    adminClient.createTopics(
                            Collections.singleton(new NewTopic(topic, partition, replicationFactor)));
            topicsResult.all().get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new AdfException("Unable to create topic - " + topic, ex);
        } finally {
            close(adminClient);
        }
    }

    public void createTopic(String topic) {
        createTopic(topic, 1, this.replicationFactor);
    }

    public boolean doesTopicExist(String topic) {
        AdminClient adminClient = null;
        try {
            adminClient = KafkaAdminClient.create(config);
            DescribeTopicsResult topicsResult = adminClient.describeTopics(Collections.singleton(topic));
            return topicsResult.all().get(10, TimeUnit.SECONDS).size() > 0;
        } catch (Exception ex) {
            if (ex.getCause() != null && ex.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw new AdfException("Unable fetch topic description- " + topic, ex);
        } finally {
            close(adminClient);
        }
    }

    public boolean deleteTopic(String topicName) {
        AdminClient adminClient = null;
        try {
            adminClient = KafkaAdminClient.create(config);
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
            return true;
        } catch (Exception ex) {
            if (ex.getCause() != null && ex.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            LOG.error("Unable to delete topic - " + topicName, ex);
            throw new AdfException("Unable to delete topic - " + topicName, ex);
        } finally {
            close(adminClient);
        }
    }

    public void close(){
        // Not implemented
    }
}
