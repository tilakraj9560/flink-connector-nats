// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import lombok.Getter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

@Getter
public class JetStreamSourceConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final int messageQueueCapacity;
    protected final boolean enableAutoAcknowledgeMessage;
    protected final Duration fetchOneMessageTimeout;
    protected final Duration fetchTimeout;
    protected final int maxFetchRecords;
    protected final Duration autoAckInterval;
    protected final Configuration configuration;
    protected final Map<String, Set<String>> subjectConsumerGroups;

    JetStreamSourceConfiguration(Map<String, Set<String>> subjectConsumerGroups,
                                 int messageQueueCapacity,
                                 boolean enableAutoAcknowledgeMessage,
                                 Duration fetchOneMessageTimeout,
                                 Duration fetchTimeout,
                                 int maxFetchRecords,
                                 Duration autoAckInterval) {
        this.subjectConsumerGroups = subjectConsumerGroups;
        this.messageQueueCapacity = messageQueueCapacity;
        this.enableAutoAcknowledgeMessage = enableAutoAcknowledgeMessage;
        this.fetchOneMessageTimeout = fetchOneMessageTimeout;
        this.fetchTimeout = fetchTimeout;
        this.maxFetchRecords = maxFetchRecords;
        this.autoAckInterval = autoAckInterval;


        configuration = new Configuration();
        configuration.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY.key(), messageQueueCapacity);
    }
}